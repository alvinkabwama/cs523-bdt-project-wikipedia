# Demo Recording Script — Wikipedia Edit Stream Pipeline

**Target duration**: 20 minutes (hard cap).
**Format**: Microsoft Teams meeting with both teammates on camera, screen
share on whoever is driving. Record the Teams meeting; upload to Microsoft
Streams; share link with professor.

**Rubric requirements**:
- ≤20 minutes
- Both teammates must be visible on camera the whole time (split-screen webcams)
- Both teammates must actively speak
- Must show data flowing end-to-end from source → Kafka → Spark → HBase → dashboard

---

## Pre-flight checklist (15 minutes before recording)

```bash
# 1. Confirm Docker is up
docker version

# 2. Bring up the lab stack
docker compose up -d

# 3. Run the one-shot setup
./setup_container.sh

# 4. Confirm Wikimedia EventStreams is reachable (no rate limits ever)
docker exec cs523bdt-lab python3 -c "
import requests
r = requests.get('https://stream.wikimedia.org/v2/stream/recentchange',
                 stream=True, timeout=5)
print('SSE status:', r.status_code)
print('first line:', next(r.iter_lines(decode_unicode=True)))
r.close()
"

# 5. Confirm no Spark apps are running (we'll start them on camera)
curl -s "http://localhost:8088/ws/v1/cluster/apps?states=RUNNING" \
  | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('apps',{}).get('app',[])))"
# Expect: 0

# 6. Open these tabs in your browser (don't refresh until on camera):
#    - http://localhost:9870   (HDFS NameNode UI)
#    - http://localhost:8088   (YARN UI)
#    - http://localhost:16010  (HBase Master UI)
#    - http://localhost:4040   (Spark UI)
#    - http://localhost:8501   (Streamlit dashboard)

# 7. Arrange 4 terminal windows in a 2x2 grid, all empty.
```

---

## Minute-by-minute outline

| Time | Section | Who speaks | What's on screen |
|---:|---|---|---|
| 0:00 – 1:00 | Intro | Both | Webcams full-frame |
| 1:00 – 3:00 | Architecture | Alvin | `docs/architecture.md` ASCII diagram |
| 3:00 – 4:30 | Data source | Mercel | Wikipedia EventStreams in a browser tab |
| 4:30 – 7:00 | Part 1 — Kafka producer | Alvin | Terminal A |
| 7:00 – 11:00 | Part 2 — Spark Structured Streaming | Both | Terminal B + YARN UI |
| 11:00 – 13:00 | Part 3 — HBase sink | Mercel | Terminal D `hbase shell` |
| 13:00 – 16:00 | Part 5 BONUS — Spark SQL join | Alvin | Terminal C |
| 16:00 – 18:30 | Part 4 — Streamlit dashboard | Mercel | Browser tab |
| 18:30 – 19:30 | What we learned | Both | Faces full-frame |
| 19:30 – 20:00 | Wrap | Both | Thank you + repo link |

---

## Detailed script

### 0:00 — Intro

**Alvin**: "Hi, I'm Alvin Leonald Kabwama."
**Mercel**: "And I'm Mercel Vubangsi."
**Alvin**: "This is our CS523 Big Data Technologies final project — a
real-time analytics pipeline for the Wikipedia edit stream. Total run
time: under twenty minutes."

### 1:00 — Architecture

**Alvin** (with `docs/architecture.md` open):
"Our source is Wikimedia EventStreams — every edit on every Wikipedia in
real time, pushed to us over **Server-Sent Events**. This is true streaming,
not REST polling. Roughly 100 events per second worldwide."

"Our producer is a Python script that keeps an open HTTP/2 connection to
Wikimedia, parses each event, and publishes one Kafka message per edit,
keyed by the wiki code."

"On the consumption side, a PySpark Structured Streaming application
running on YARN subscribes to the Kafka topic and runs three queries: a
live snapshot per wiki, a per-wiki edit count over 1-minute tumbling
windows, and a per-wiki average length delta over the same window."

"For the +2 bonus, a second Spark application joins each incoming event
against a hand-authored language metadata file on HDFS — wiki code maps
to language name, continent, region, and primary country."

"The dashboard reads HBase over Thrift and auto-refreshes every five
seconds."

### 3:00 — Data source

**Mercel** (opens `https://stream.wikimedia.org/v2/stream/recentchange` in a browser):
"This is the raw Server-Sent Events feed. Every line that starts with
`data:` is one JSON-encoded edit event. You can see them streaming past
in real time — these are Wikipedia edits happening right now, all over
the world, in every language."

"Wikimedia operates this feed publicly and for free. No API key, no rate
limit. It's the canonical streaming source the Wikimedia Foundation uses
to teach this exact pattern."

### 4:30 — Part 1: Kafka producer

**Alvin** [Terminal A]:
```bash
docker exec -it cs523bdt-lab python3 /opt/project/producer/producer.py
```

(Wait ~10 seconds for `sent=200 rate=...` log line.)

"Our producer is subscribing to the SSE feed, parsing each event, trimming
to the fields we care about, and publishing one Kafka message per event
keyed by wiki. You can see the rate climbing — typically 50 to 200 events
per second depending on time of day."

[Open a 5th terminal to peek into Kafka:]
```bash
docker exec -it kafka-server kafka-console-consumer \
  --bootstrap-server kafka-server:9092 \
  --topic wikipedia-raw \
  --max-messages 1 --timeout-ms 5000
```

"There's a real edit — title, user, namespace, byte delta, the wiki it
came from. That's Part 1, Kafka ingestion."

### 7:00 — Part 2: Spark Structured Streaming

**Mercel** [Terminal B]:
```bash
docker exec -it cs523bdt-lab spark-submit \
  --master yarn --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
  /opt/project/spark_app/stream_processor.py
```

(Wait for YARN ACCEPTED → RUNNING. Switch to YARN UI.)

**Alvin** [YARN UI at `http://localhost:8088`]:
"There's our application — `WikipediaEditsProcessor`, state RUNNING. YARN
allocated three containers: one ApplicationMaster and two executors."

(Switch to Spark UI at `http://localhost:4040`.)

**Alvin**:
"In the Spark UI you can see our three streaming queries — live snapshot,
per-wiki count, per-wiki average length delta. Each one has its own HDFS
checkpoint directory for exactly-once semantics across restarts."

**Mercel**:
"Watermarked at 30 seconds — we promise Spark no event arrives more than
30 seconds late, which lets it close window state and free memory."

### 11:00 — Part 3: HBase sink

**Mercel** [Terminal D]:
```bash
docker exec -it cs523bdt-lab hbase shell
```
```
hbase> count 'wikipedia_live'
hbase> count 'wikipedia_agg'
```

"`wikipedia_live` has the latest edit per wiki. `wikipedia_agg` has the
windowed metrics. Let's look at one of each."

```
hbase> scan 'wikipedia_live', {LIMIT => 1}
hbase> scan 'wikipedia_agg', {ROWPREFIXFILTER => 'enwiki|', LIMIT => 2}
```

"Row key is `<wiki>|<reverse-epoch>`. Reverse-epoch means newer windows
sort before older ones, so 'last N minutes for English Wikipedia' is a
tight prefix scan with zero ORDER BY cost."

### 13:00 — Part 5 BONUS: Spark SQL static-data join

**Alvin** [Terminal C]:
```bash
docker exec -it cs523bdt-lab hdfs dfs -cat /user/static/language_metadata.json | head -3
```

"There's our static data — 85-row JSON Lines file mapping wiki codes to
language, continent, region, and primary country. Now we submit the
enrich job:"

```bash
docker exec -it cs523bdt-lab spark-submit \
  --master yarn --deploy-mode client \
  --executor-memory 768m --num-executors 1 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
  /opt/project/spark_app/enrich_with_join.py
```

(Wait for the `[enrich-wikipedia] join logical plan:` line.)

"Catalyst chose a **BroadcastHashJoin** — the small static side gets
shipped to every executor, no shuffle. That's the most efficient plan
for a stream-static join."

(Wait ~20 seconds for `wikipedia_enriched` to populate.)
```
hbase> scan 'wikipedia_enriched', {LIMIT => 1}
```

"Each enriched row has the original edit data PLUS four new columns from
the join — language, continent, region, and primary country. That's the
+2 bonus, end to end."

### 16:00 — Part 4: Streamlit dashboard

**Mercel** [browser, `http://localhost:8501`]:

"Here's the dashboard."

"Top row: four live metrics — total edits in the last minute across every
wiki, number of active wikis, the most active wiki right now, and the
bot-vs-human ratio of edits we've captured."

"Below that: top wikis by edits in the last minute, and average length
delta — positive means wikis are growing on average, negative means net
deletions."

"Trend chart: edits per minute for whichever wiki you type in the
sidebar."

"And the bonus view at the bottom: continent and language breakdowns
from the Spark SQL join, with a sample-rows table showing exactly which
real-world edits the enriched stream contains."

(Type `frwiki` in the sidebar to demonstrate interactivity.)

### 18:30 — What we learned

**Alvin**:
"The most striking thing about this source is that it's the genuine
push-streaming case. The crypto and flights variants we considered both
involve our producer driving the cadence — even when we use WebSockets
for Binance, we throttle. Wikimedia's SSE feed is the source actually
choosing when to send each event."

**Mercel**:
"On the architecture side, putting Kafka between the producer and
everything downstream meant we could swap out the source entirely while
keeping Spark, HBase, and the dashboard unchanged. That decoupling is
exactly the property the lecturer described in the project transcript."

### 19:30 — Wrap

**Both**:
"Repo is at github.com/alvinkabwama/cs523-bdt-project-wikipedia.
`docker compose up -d` and `./setup_container.sh` and four terminal
commands. Thanks for watching."
