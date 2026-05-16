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
| 2 | 1-min tumbling, `count("*") per wiki` | `wikipedia_agg` (col `count`) |
| 3 | 1-min tumbling, `avg(length_delta) per wiki` | `wikipedia_agg` (col `avg_delta`) |

**Choice of aggregation, by design.** Q2 uses exact `count("*")` rather
than `approx_count_distinct(...)`. Each Kafka message represents one
unique edit event, so the natural unit is events-per-minute and exact
counting is both correct and bounded in state. (Contrast: the OpenSky
flights pipeline uses `approx_count_distinct("icao24")` because it
needs distinct-aircraft counts and Spark Structured Streaming does not
support exact `countDistinct` in streaming aggregations.) Q3's
`avg(length_delta)` is a per-wiki signal of whether content is being
added (positive average) or removed (negative average) over the
window.

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

- **Wikimedia User-Agent policy is enforced.** Wikimedia rejects
  Server-Sent Events connections from clients that send the default
  `python-requests/X.Y` User-Agent header — they return HTTP 403. The
  producer therefore sends an identifying header that includes the
  team's contact email, in the format Wikimedia's
  [User-Agent policy](https://meta.wikimedia.org/wiki/User-Agent_policy)
  requires:
  `CS523-Bigdata-Student-Project/0.1 (alkaleos10@gmail.com) kafka-python/2.3 requests/2.34`.
  This is a real-world constraint that operates outside HTTP status
  codes and is not obvious until you trip over it.
- The Wikimedia SSE feed is reliable and has no rate limit, but
  occasionally drops connections. The producer reconnects every 5 seconds
  on failure.
- YARN has 8 GB total. The two Spark applications (main + enrich) fit
  only if the enrich app is launched with
  `--executor-memory 768m --num-executors 1`. Otherwise the second
  application sits in `ACCEPTED` state waiting for resources that never
  arrive. The README's "Caveats" section spells this out for the grader.
- Network egress: the producer is a long-lived HTTP/2 connection to
  Wikimedia. Restarting it does not lose events that have already been
  published to Kafka; it only resumes from "now" upstream. Events that
  happened during the reconnect window are lost upstream of Kafka.
- **HBase state across container restarts.** HBase persists region
  assignments and the location of `hbase:meta` in ZooKeeper. If the lab
  container is recreated but ZooKeeper is not, HMaster waits forever for
  the ghost RegionServer that ZooKeeper still believes is alive. Fix:
  `docker exec zookeeper-server bash -c "echo deleteall /hbase | zkCli.sh -server localhost:2181"`
  followed by an HBase restart. Documented in `docs/demo-script.md`
  under "Backup plans".

---

## 6. Dashboard implementation choices

The dashboard sits outside the streaming-pipeline architecture but
deserves its own design notes because several decisions were
deliberate.

### 6.1 Pretty-name resolution for wiki codes

The Kafka topic, the HBase row keys, and the static lookup file all
use raw Wikimedia codes (`enwiki`, `commonswiki`, `frwikibooks`). The
dashboard never shows these to the user. `dashboard/app.py` resolves
any code into a human-readable name via a three-layer fallback in
`pretty_wiki(code)`:

1. **`WIKI_DISPLAY_NAMES`** — a curated dict of ~90 specific codes mapped
   to bespoke names. Covers the big-name wikis and the cross-project
   wikis (Commons, Wikidata, Meta, MediaWiki documentation, Wikispecies,
   Incubator, Outreach, central login).
2. **`LANGUAGE_CODE_TO_NAME` + `PROJECT_SUFFIX_TO_NAME`** — smart parse.
   Splits the code as `<lang><project>` (longest project suffix wins so
   `wiktionary` beats `wiki`) and combines the two halves. Covers ~150
   ISO-639 language codes and 8 project suffixes (Wikipedia,
   Wiktionary, Wikibooks, Wikiquote, Wikisource, Wikinews, Wikiversity,
   Wikivoyage). Example: `ckbwiki` → "Central Kurdish (Sorani) Wikipedia".
3. **Raw code** — only if both lookups miss. Rare.

The same approach makes the **sidebar trend-wiki picker** a `selectbox`
rather than a `text_input`. The user picks a wiki by its pretty name;
a reverse-lookup dict (`pretty_to_code`) resolves it back to the raw
code internally for the HBase lookup.

### 6.2 Two extra breakdowns derived from `wikipedia_live`

In addition to the rubric-required sections, the dashboard renders two
breakdowns that come for free from the live snapshot table:

- **Edit types observed** — counts how many of the currently-tracked
  wikis received each event type (`edit`, `new`, `log`, `categorize`,
  `external`) in the most recent batch.
- **Wikipedia namespaces observed** — counts how many wikis had at
  least one edit in each namespace (Article, Article talk, Category,
  User, Module, etc.).

Both come from scanning the `info:type` and `info:namespace` columns
in `wikipedia_live` — no extra Spark query, no extra HBase table.
This is a small example of how a wide-column store with multiple
column families per row pays off: the dashboard can aggregate on any
field cheaply because each family is stored separately.

### 6.3 Live bonus metric: "Latest edit per language"

The bonus enrichment table `wikipedia_enriched` is keyed by `wiki`, so
each row is overwritten on every cycle with the most recent edit for
that wiki. The dashboard scans this table, groups by `ref:language`,
and renders the most-recently-observed page title and editor for each
language. Because the underlying cells are overwritten every cycle,
this view is the most visibly "alive" part of the page when the
enrich Spark application is running.

### 6.4 Color theme

The dashboard uses a custom CSS block to apply an admin-portal
aesthetic on top of Streamlit's default widgets: dark navy sidebar
(#1d3a52), light grey main background (#f3f4f6), white metric cards
with rounded corners + a 4-pixel colored left-accent bar that cycles
through blue / grey / orange / teal across columns. No restructuring
of Streamlit's widget set, just CSS overrides targeting Streamlit's
testid attributes.

---

## 7. Versions in use

| Component | Version |
|---|---|
| Hadoop / HDFS / YARN | 3.2.1 |
| Spark | 3.1.2 (Scala 2.12) |
| Kafka | 3.4.0 (broker, via Confluent image v7.4.0) |
| HBase | 2.2.0 |
| ZooKeeper | 3.8 |
| Java | 1.8.0_482 (Temurin) |
| Python | 3.12 |
| `kafka-python` | 2.3.x |
| `requests` | 2.34.x |
| `happybase` | 1.3.x |
| `pandas` | 2.x |
| `altair` | 5.x |
| Streamlit | 1.28+ (uses `width="stretch"` and `st.container(border=True)` which arrived in 1.29) |
| Container image | `mmukadam/cs523bdt-lab:v4.0` (course-provided, bundles all of the above except Streamlit) |
