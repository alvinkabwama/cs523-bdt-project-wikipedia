"""BONUS (+2pt) -- Spark SQL join for the Wikipedia stream.

Joins the live Kafka edit stream against a hand-authored language-metadata
lookup on HDFS (hdfs:///user/static/language_metadata.json), attaching
`language`, `continent`, `region`, and `primary_country` to every edit
event, and writes enriched rows into HBase table `wikipedia_enriched`.

Submit from inside the lab container:

    spark-submit \\
        --master yarn --deploy-mode client \\
        --executor-memory 768m --num-executors 1 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \\
        /opt/project/spark_app/enrich_with_join.py
"""

import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast, col, from_json, timestamp_seconds,
)
from pyspark.sql.types import (
    BooleanType, LongType, StringType, StructField, StructType,
)

KAFKA_BOOTSTRAP   = "kafka-server:9092"
KAFKA_TOPIC       = "wikipedia-raw"
CHECKPOINT_ROOT   = "hdfs:///user/spark/checkpoints"
STATIC_HDFS_PATH  = "hdfs:///user/static/language_metadata.json"
HBASE_HOST        = "localhost"
HBASE_THRIFT_PORT = 9090

EVENT_SCHEMA = StructType([
    StructField("id",            LongType()),
    StructField("type",          StringType()),
    StructField("wiki",          StringType()),
    StructField("title",         StringType()),
    StructField("namespace",     LongType()),
    StructField("user",          StringType()),
    StructField("bot",           BooleanType()),
    StructField("minor",         BooleanType()),
    StructField("comment",       StringType()),
    StructField("timestamp",     LongType()),
    StructField("server_url",    StringType()),
    StructField("server_name",   StringType()),
    StructField("length_old",    LongType()),
    StructField("length_new",    LongType()),
    StructField("length_delta",  LongType()),
    StructField("revision_old",  LongType()),
    StructField("revision_new",  LongType()),
    StructField("ingestion_ts",  LongType()),
])


def _b(v):
    if v is None:
        return b""
    if isinstance(v, bool):
        return b"true" if v else b"false"
    return str(v).encode("utf-8")


def write_enriched(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return
    conn = happybase.Connection(HBASE_HOST, port=HBASE_THRIFT_PORT)
    try:
        tbl = conn.table("wikipedia_enriched")
        with tbl.batch(batch_size=500) as b:
            for r in rows:
                if not r.wiki:
                    continue
                b.put(r.wiki.encode("utf-8"), {
                    b"info:type":            _b(r.type),
                    b"info:title":           _b(r.title),
                    b"info:user":            _b(r.user),
                    b"info:bot":             _b(r.bot),
                    b"stats:length_delta":   _b(r.length_delta),
                    b"ref:language":         _b(r.language),
                    b"ref:continent":        _b(r.continent),
                    b"ref:region":           _b(r.region),
                    b"ref:primary_country":  _b(r.primary_country),
                })
    finally:
        conn.close()


def main():
    spark = (
        SparkSession.builder
        .appName("WikipediaEnrichJoin")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Static side: hand-authored language metadata on HDFS ---
    static_df = (
        spark.read.json(STATIC_HDFS_PATH)
        .select("wiki", "language", "continent", "region", "primary_country")
        .cache()
    )
    n = static_df.count()
    print(f"[enrich-wikipedia] loaded {n} language rows from {STATIC_HDFS_PATH}")
    static_df.show(5, truncate=False)

    # --- Streaming side ---
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )
    parsed = (
        raw.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", timestamp_seconds(col("ingestion_ts")))
        .withWatermark("event_time", "30 seconds")
    )

    # --- Spark SQL broadcast join (DataFrame API) ---
    enriched = parsed.join(
        broadcast(static_df),
        on="wiki",
        how="left",
    ).select(
        "wiki", "type", "title", "user", "bot", "length_delta",
        "language", "continent", "region", "primary_country",
    )

    print("[enrich-wikipedia] join logical plan:")
    enriched.explain(extended=False)

    q = (
        enriched.writeStream
        .queryName("enrich_wikipedia_to_hbase")
        .outputMode("append")
        .foreachBatch(write_enriched)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/enrich_wikipedia_to_hbase")
        .trigger(processingTime="15 seconds")
        .start()
    )
    q.awaitTermination()


if __name__ == "__main__":
    main()
