# CS523 Final Project (WIKIPEDIA variant) — Architecture & Design

**Project**: Real-time Wikipedia recent-changes analytics pipeline
**Authors**: Alvin Leonald Kabwama, Mercel Vubangsi

---

## 1. Executive summary

End-to-end real-time pipeline that:

1. **Subscribes** to the Wikimedia Foundation's public Server-Sent Events feed
   (`https://stream.wikimedia.org/v2/stream/recentchange`), which pushes
   every edit on every Wikipedia and sister project as it happens —
   typically 50–200 events per second worldwide.
2. **Publishes** each event individually to Kafka topic `wikipedia-raw`,
   keyed by `wiki` (so all edits to `enwiki` land on the same partition).
3. **Processes** the stream with a PySpark Structured Streaming application
   running on YARN, with three concurrent queries: live snapshot,
   per-wiki edit count over 1-minute windows, and per-wiki average length
   delta (bytes added or removed) over 1-minute windows.
4. **Enriches** the stream (bonus) by joining against a hand-authored
   language-metadata file on HDFS (wiki code → language name, continent,
   region, primary country) using Spark SQL with an explicit broadcast hint.
5. **Persists** results in three HBase tables: `wikipedia_live`,
   `wikipedia_agg`, and `wikipedia_enriched`.
6. **Visualises** the data on a Streamlit dashboard that reads HBase over
   Thrift (port 9090) and auto-refreshes every five seconds.

Why this source: Wikimedia EventStreams is the **only** source of the
three project variants where the *upstream* push is genuinely real-time
(not REST polling). It is also free forever, has no rate limit, no API
key, and is the canonical example used by the Wikimedia Foundation to
teach streaming-data concepts.

---

## 2. High-level architecture

```
                  ┌────────────────────────────────┐
                  │  Wikimedia EventStreams (SSE)  │
                  │  stream.wikimedia.org/v2/      │
                  │     stream/recentchange        │
                  └─────────────┬──────────────────┘
                                │  ~100 events / sec
                                │  (every Wikipedia edit globally)
                                ▼
                  ┌─────────────────────────────────┐
                  │  Python SSE subscriber          │
                  │  producer/producer.py           │
                  │                                 │
                  │  - parses each line of the SSE  │
                  │    stream                       │
                  │  - trims to fields we keep      │
                  │  - publishes one Kafka message  │
                  │    per event, keyed by wiki     │
                  └─────────────┬───────────────────┘
                                │   one JSON message per edit
                                ▼
                  ┌─────────────────────────────────┐
                  │  Kafka topic: wikipedia-raw     │
                  │  partitions=1, replication=1    │
                  └─────────────┬───────────────────┘
                                │
                                ▼
        ┌──────────────────────────────────────────────────────┐
        │     PySpark Structured Streaming on YARN             │
        │     spark_app/stream_processor.py                    │
        │                                                      │
        │  read_stream(kafka)                                  │
        │      │                                               │
        │      ├── parseJson + watermark(ingestion_ts, 30s)    │
        │      │                                               │
        │      ├── Q1: live snapshot                           │
        │      │     -> HBase wikipedia_live  (key=wiki)       │
        │      │                                               │
        │      ├── Q2: 1-min tumbling, count per wiki          │
        │      │     -> HBase wikipedia_agg (m:count)          │
        │      │                                               │
        │      └── Q3: 1-min tumbling, avg length_delta        │
        │            -> HBase wikipedia_agg (m:avg_delta)      │
        │                                                      │
        │  + (bonus) enrich_with_join.py:                      │
        │    broadcast join with HDFS language_metadata.json   │
        │    -> HBase wikipedia_enriched (key=wiki)            │
        └──────────────────────────────┬───────────────────────┘
                                       │
                          ┌────────────┼────────────┐
                          ▼            ▼            ▼
                ┌────────────────┐ ┌──────────────┐ ┌──────────────────┐
                │ wikipedia_live │ │ wikipedia_   │ │ wikipedia_       │
                │                │ │   agg        │ │ enriched         │
                │ latest edit    │ │ windowed     │ │ edits joined     │
                │ per wiki       │ │ counts +     │ │ with language    │
                │                │ │ length deltas│ │ metadata         │
                └────────────────┘ └──────────────┘ └──────────────────┘
                                       │
                                       │  happybase Thrift (port 9090)
                                       ▼
                           ┌──────────────────────┐
                           │  Streamlit dashboard │
                           │  dashboard/app.py    │
                           └──────────────────────┘
```

---

## 3. Data model

### 3.1 Kafka

- **Topic**: `wikipedia-raw`
- **Partitions**: 1 · **Replication**: 1
- **Key**: `wiki` (e.g., `enwiki`, `frwiki`, `commonswiki`)
- **Value**: UTF-8 JSON, one edit event per message:

```json
{
  "id": 1234567890,
  "type": "edit",
  "wiki": "enwiki",
  "title": "Big Data",
  "namespace": 0,
  "user": "ExampleUser",
  "bot": false,
  "minor": false,
  "comment": "Fixed citation formatting",
  "timestamp": 1709300000,
  "server_url": "https://en.wikipedia.org",
  "server_name": "en.wikipedia.org",
  "length_old": 12345,
  "length_new": 12389,
  "length_delta": 44,
  "revision_old": 1138291011,
  "revision_new": 1138291012,
  "ingestion_ts": 1709300010
}
```

### 3.2 HBase

| Table | Row key | Column families | Purpose |
|---|---|---|---|
| `wikipedia_live` | `wiki` | `info`, `stats`, `meta` | Latest edit per wiki, overwritten each cycle |
| `wikipedia_agg` | `<wiki>\|<reverse_window_end_epoch>` | `m` | Per-wiki 1-minute windowed metrics (count, avg_delta); newest windows scan first because reverse-epoch keys sort that way |
| `wikipedia_enriched` | `wiki` | `info`, `stats`, **`ref`** | Live stream joined with `language_metadata.json`; new `ref` family carries `language`, `continent`, `region`, `primary_country` |

### 3.3 Static enrichment dataset

`data/static/language_metadata.json`, JSON Lines format, ~85 rows. Each row:

```json
{"wiki":"enwiki","language":"English","continent":"Multiple","region":"Global","primary_country":"United States"}
```

Stored at `hdfs:///user/static/language_metadata.json`. Loaded once at
Spark application start, cached in memory, broadcast to every executor.

---

## 4. Spark application design

### 4.1 Why three queries

The rubric asks for "meaningful transformations or aggregations." Three
queries demonstrates mastery of Spark Structured Streaming without
duplicating logic:

| # | Logic | Output |
|---|---|---|
| 1 | Live snapshot per `wiki`, no aggregation | `wikipedia_live` |
| 2 | 1-min tumbling, `count(*) per wiki` | `wikipedia_agg` (col `count`) |
| 3 | 1-min tumbling, `avg(length_delta) per wiki` | `wikipedia_agg` (col `avg_delta`) |

### 4.2 Watermarking

```python
df.withWatermark("event_time", "30 seconds")
```

`event_time` is parsed from `ingestion_ts` (seconds since epoch, anchored
to the producer's wall-clock at publish time). This keeps the watermark
monotonic even if Wikipedia's stream momentarily delivers events out of
order, and lets Spark close windows + free state.

### 4.3 Checkpointing

Each query has its own HDFS checkpoint directory under
`hdfs:///user/spark/checkpoints/q*_*`, giving exactly-once semantics
across restarts.

### 4.4 HBase write path

Same `foreachBatch` + `happybase` pattern used in the other two project
variants. Batches are small (~100 events per 10s for Q1, a few hundred
windowed rows for Q2/Q3) so a driver-side `.collect()` is safe.

---

## 5. Operational notes

- The Wikimedia SSE feed is reliable and has no rate limit, but
  occasionally drops connections. The producer reconnects every 5 seconds
  on failure.
- YARN has 8 GB total. The two Spark applications (main + enrich) fit if
  the enrich app is launched with `--executor-memory 768m --num-executors 1`.
- Network egress: the producer is a long-lived HTTP/2 connection to
  Wikimedia. Restarting it does not lose events that have already been
  published to Kafka; it only resumes from "now" upstream.

---

## 6. Versions in use

| Component | Version |
|---|---|
| Hadoop / HDFS / YARN | 3.2.1 |
| Spark | 3.1.2 (Scala 2.12) |
| Kafka | 3.4.0 (broker) |
| HBase | 2.2.0 |
| Java | 1.8.0_482 (Temurin) |
| Python | 3.12 |
| `kafka-python` | 2.3.x |
| `requests` | 2.34.x |
| `happybase` | 1.3.x |
| Streamlit | 1.28+ |
