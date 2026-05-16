"""Wikipedia recent-changes Spark Structured Streaming processor.

Reads JSON edit events from Kafka `wikipedia-raw`, applies a 30s watermark,
and runs three concurrent streaming queries with HBase sinks:

    Q1. Live snapshot per wiki              -> HBase wikipedia_live   (key=wiki)
    Q2. Per-wiki edit count, 1-min window   -> HBase wikipedia_agg    (key=wiki|reverse_epoch, m:count)
    Q3. Per-wiki avg length delta, 1-min    -> HBase wikipedia_agg    (m:avg_delta)

Submit from inside the lab container:

    spark-submit \\
        --master yarn --deploy-mode client \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \\
        /opt/project/spark_app/stream_processor.py
"""

import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, from_json, timestamp_seconds, unix_timestamp, window,
)
from pyspark.sql.types import (
    BooleanType, LongType, StringType, StructField, StructType,
)

KAFKA_BOOTSTRAP = "kafka-server:9092"
KAFKA_TOPIC = "wikipedia-raw"
CHECKPOINT_ROOT = "hdfs:///user/spark/checkpoints"
HBASE_HOST = "localhost"
HBASE_THRIFT_PORT = 9090
LONG_MAX = 9_223_372_036_854_775_807

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


def write_live(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return
    conn = happybase.Connection(HBASE_HOST, port=HBASE_THRIFT_PORT)
    try:
        tbl = conn.table("wikipedia_live")
        with tbl.batch(batch_size=500) as b:
            for r in rows:
                if not r.wiki:
                    continue
                b.put(r.wiki.encode("utf-8"), {
                    b"info:type":         _b(r.type),
                    b"info:title":        _b(r.title),
                    b"info:user":         _b(r.user),
                    b"info:namespace":    _b(r.namespace),
                    b"info:server_name":  _b(r.server_name),
                    b"stats:length_old":  _b(r.length_old),
                    b"stats:length_new":  _b(r.length_new),
                    b"stats:length_delta":_b(r.length_delta),
                    b"meta:timestamp":    _b(r.timestamp),
                    b"meta:bot":          _b(r.bot),
                    b"meta:minor":        _b(r.minor),
                    b"meta:ingestion_ts": _b(r.ingestion_ts),
                })
    finally:
        conn.close()


def write_agg(batch_df, batch_id, qualifier):
    rows = batch_df.collect()
    if not rows:
        return
    conn = happybase.Connection(HBASE_HOST, port=HBASE_THRIFT_PORT)
    try:
        tbl = conn.table("wikipedia_agg")
        with tbl.batch(batch_size=500) as b:
            for r in rows:
                window_end_epoch = int(r.window_end_epoch)
                reverse = LONG_MAX - window_end_epoch
                key = f"{r.wiki}|{reverse:020d}".encode("utf-8")
                cells = {
                    f"m:{qualifier}".encode(): _b(r[qualifier]),
                    b"m:window_end":           _b(window_end_epoch),
                    b"m:wiki":                 _b(r.wiki),
                }
                b.put(key, cells)
    finally:
        conn.close()


def write_agg_count(batch_df, batch_id):
    return write_agg(batch_df, batch_id, "count")


def write_agg_avg_delta(batch_df, batch_id):
    return write_agg(batch_df, batch_id, "avg_delta")


def main():
    spark = (
        SparkSession.builder
        .appName("WikipediaEditsProcessor")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("d"))
        .select("d.*")
        # event_time anchored to ingestion_ts (seconds since epoch) so the
        # watermark is monotonic even if Wikipedia events arrive out of order.
        .withColumn("event_time", timestamp_seconds(col("ingestion_ts")))
    )
    with_wm = parsed.withWatermark("event_time", "30 seconds")

    # ===== Q1: live snapshot per wiki =====
    q1 = (
        with_wm.writeStream
        .queryName("q1_wikipedia_live")
        .outputMode("append")
        .foreachBatch(write_live)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/q1_wikipedia_live")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ===== Q2: per-wiki edit count per 1-min window =====
    counts = (
        with_wm.groupBy(window(col("event_time"), "1 minute"), col("wiki"))
        .agg(count("*").alias("count"))
        .withColumn("window_end_epoch", unix_timestamp(col("window.end")))
    )
    q2 = (
        counts.writeStream
        .queryName("q2_wikipedia_count")
        .outputMode("update")
        .foreachBatch(write_agg_count)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/q2_wikipedia_count")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ===== Q3: per-wiki avg length delta per 1-min window =====
    avg_delta = (
        with_wm.filter(col("length_delta").isNotNull())
        .groupBy(window(col("event_time"), "1 minute"), col("wiki"))
        .agg(avg("length_delta").alias("avg_delta"))
        .withColumn("window_end_epoch", unix_timestamp(col("window.end")))
    )
    q3 = (
        avg_delta.writeStream
        .queryName("q3_wikipedia_avg_delta")
        .outputMode("update")
        .foreachBatch(write_agg_avg_delta)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/q3_wikipedia_avg_delta")
        .trigger(processingTime="30 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
