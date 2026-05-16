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

Run these in one PowerShell window in the repo directory. They are NOT
part of the recorded demo — they set the stage so the recording starts
with the lab container up, the setup done, the HBase tables created
(but empty), and no Spark applications running. The first thing the
camera will see is the empty HBase tables, then the full pipeline
brought up live.

```powershell
cd "C:\Users\user\Documents\MIU DE Courses\Big Data\cs523-bdt-project-wikipedia"

# 1. Confirm Docker Desktop is running
docker version

# 2. Bring up the lab stack (4 containers: zookeeper, kafka, postgres, cs523bdt-lab)
docker compose up -d
docker ps --format "table {{.Names}}`t{{.Status}}"
# Expect: all four containers in state "Up"

# 3. Run the one-shot setup (installs Python deps, starts YARN+HBase+Thrift,
#    creates Kafka topic + 3 HBase tables, uploads static JSON to HDFS).
#    Idempotent -- safe to re-run.
bash .\setup_container.sh

# 4. Confirm Wikimedia EventStreams is reachable
docker exec cs523bdt-lab python3 -c "import requests; r = requests.get('https://stream.wikimedia.org/v2/stream/recentchange', stream=True, headers={'User-Agent': 'CS523-preflight'}, timeout=5); print('SSE status:', r.status_code); r.close()"
# Expect: SSE status: 200

# 5. Confirm no Spark apps are running (we will start them on camera)
curl -s "http://localhost:8088/ws/v1/cluster/apps?states=RUNNING"
# Expect: {"apps":null} or {"apps":{}}

# 6. Confirm HBase tables exist but are empty
docker exec cs523bdt-lab bash -c "echo list | hbase shell 2>/dev/null | grep -E 'wikipedia_'"
# Expect: wikipedia_agg, wikipedia_enriched, wikipedia_live

# 7. Kill any leftover streamlit / producer processes from prior runs
docker exec cs523bdt-lab pkill -9 -f streamlit
docker exec cs523bdt-lab pkill -9 -f "producer.py"

# 8. Open these tabs in your browser (don't switch to them until on camera):
#    - http://localhost:8088   (YARN ResourceManager UI)
#    - http://localhost:16010  (HBase Master UI)
#    - http://localhost:8501   (Streamlit dashboard -- will 404 until we start it)
#    - https://stream.wikimedia.org/v2/stream/recentchange  (the raw SSE feed)

# 9. Arrange 5 PowerShell windows in a 2x3 grid (one extra for hbase shell).
#    All empty, all already cd'd into the repo directory.
```

### About the .bat helper files

Inside the repo root there are four batch files. Each one wraps a
single `docker exec -it` command so that PowerShell does not split the
command across lines (which would otherwise mangle the package URL
and fail). All four pipeline commands in this script are run via
these wrappers:

| File | What it runs |
|---|---|
| `1-run-producer.bat`     | Producer (Wikimedia SSE -> Kafka) |
| `2-run-spark-stream.bat` | Spark stream processor on YARN |
| `3-run-spark-enrich.bat` | Spark SQL bonus enrichment join on YARN |
| `4-run-dashboard.bat`    | Streamlit dashboard |

Each file is a single line. Press Ctrl+C in any of them to stop that
component. The container itself keeps running.

---

## Minute-by-minute outline

The recording has a clear **before -> during -> after** arc. We open
the HBase shell on camera FIRST to prove the tables are empty (the
"before" state). Then we bring up the four pipeline components in
order. Then we re-open the HBase shell to show the same tables now
full (the "after" state). That contrast is the strongest visual
proof that the pipeline is genuinely running end-to-end.

| Time | Section | Who speaks | What's on screen |
|---:|---|---|---|
| 0:00 – 1:00   | Intro                                       | Both    | Webcams full-frame |
| 1:00 – 3:00   | Architecture overview                        | Alvin   | `docs/architecture.md` ASCII diagram |
| 3:00 – 4:30   | Data source: Wikimedia EventStreams (SSE)    | Mercel  | Raw SSE feed in a browser tab |
| 4:30 – 6:30   | **BEFORE state**: empty HBase tables         | Mercel  | Terminal E -- `hbase shell` |
| 6:30 – 8:30   | Part 1 -- Kafka producer (start on camera)   | Alvin   | Terminal A -- `.\1-run-producer.bat` |
| 8:30 – 11:00  | Part 2 -- Spark Structured Streaming         | Both    | Terminal B + YARN UI at `:8088` |
| 11:00 – 13:00 | **AFTER state**: HBase tables now populated  | Mercel  | Terminal E re-scan |
| 13:00 – 16:00 | Part 5 BONUS -- Spark SQL static-data join   | Alvin   | Terminal C + the `BroadcastHashJoin` plan |
| 16:00 – 18:30 | Part 4 -- Streamlit dashboard                | Mercel  | Terminal D + browser at `:8501` |
| 18:30 – 19:30 | What we learned                              | Both    | Faces full-frame |
| 19:30 – 20:00 | Wrap                                         | Both    | Repo URL + thanks |

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

### 4:30 — BEFORE state: empty HBase tables

This is the strongest "before" moment in the demo. We have the
container running and the setup script has already run — so the
three HBase tables EXIST with the right schema — but no data has
been written yet. Showing the empty tables on camera makes the
contrast at 11:00 (when we re-scan and they are full) dramatic.

**Mercel** [Terminal E -- open `hbase shell` once and keep it open
all the way through 13:00]:

```powershell
docker exec -it cs523bdt-lab hbase shell
```

Wait for the `hbase(main):001:0>` prompt, then:

```
list
```

Expected output (the three tables exist):
```
TABLE
wikipedia_agg
wikipedia_enriched
wikipedia_live
3 row(s)
```

**Mercel**: "Our setup script already ran and created these three HBase
tables. Right now they have no data in them — let me prove it."

```
count 'wikipedia_live'
```
Expected: `0 row(s)`

```
count 'wikipedia_agg'
```
Expected: `0 row(s)`

```
count 'wikipedia_enriched'
```
Expected: `0 row(s)`

**Mercel**: "All three are empty. Now let's look at the schema of one
of them — `wikipedia_live`."

```
describe 'wikipedia_live'
```

Expected output shows column families `info`, `stats`, `meta` with
`VERSIONS => 1` each.

**Mercel**: "Three column families — `info` for the identifier fields,
`stats` for the byte-length numbers, `meta` for timestamps and flags.
HBase stores each family in its own set of HFiles, which is why
denormalizing by access pattern is the right design here. Now let's
start the pipeline and watch this empty table fill up."

Keep this `hbase shell` open in Terminal E. We will come back to it
at 11:00 and 13:00.

### 6:30 — Part 1: Kafka producer

**Alvin** [Terminal A]:

```powershell
.\1-run-producer.bat
```

(That batch file wraps the single command
`docker exec -it cs523bdt-lab python3 /opt/project/producer/producer.py`.
Wait ~15 seconds for the first `sent=200 rate=X.X events/sec` log line.)

**Alvin**: "Our producer is subscribing to the Wikimedia Server-Sent
Events feed, parsing each event, trimming it to the fields we care
about, and publishing one Kafka message per event, keyed by the wiki
code. You can see the rate — typically 50 to 200 events per second
worldwide depending on time of day. Every line you see is one real
Wikipedia edit that just happened somewhere in the world."

(Optional, only if time allows: peek into Kafka directly.)
```powershell
docker exec -it kafka-server kafka-console-consumer --bootstrap-server kafka-server:9092 --topic wikipedia-raw --max-messages 1 --timeout-ms 5000
```

**Alvin**: "There's a real edit — wiki, title, user, namespace, byte
delta. That is Part 1, Kafka ingestion."

### 8:30 — Part 2: Spark Structured Streaming

**Mercel** [Terminal B]:

```powershell
.\2-run-spark-stream.bat
```

(That batch file wraps the full `spark-submit --master yarn --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /opt/project/spark_app/stream_processor.py`. Wait ~45 seconds for the YARN ACCEPTED -> RUNNING transition.)

While Spark starts, switch to the YARN UI in a browser tab.

**Alvin** [browser at `http://localhost:8088`]: "There's our
application -- `WikipediaEditsProcessor`, state RUNNING. YARN
allocated three containers: one ApplicationMaster and two executors,
in total about 5 GB of memory."

(Switch to the Spark UI at `http://localhost:4040`.)

**Alvin**: "In the Spark UI we can see our three concurrent streaming
queries -- live snapshot per wiki, per-wiki edit count over 1-minute
tumbling windows, and per-wiki average byte delta over the same
window. Each one has its own HDFS checkpoint directory for
exactly-once semantics across restarts."

**Mercel**: "Watermarked at 30 seconds -- we promise Spark no event
arrives more than 30 seconds late, which lets Structured Streaming
close window state and free memory. Without a watermark, the windowed
aggregations would grow state forever."

### 11:00 — AFTER state: HBase tables now populated (Part 3 -- HBase sink)

By now Spark has been running for ~2 minutes. The same three tables
that were empty at 4:30 should now have hundreds or thousands of rows.

**Mercel** [back in Terminal E -- the same `hbase shell` window that
was open since 4:30]:

```
count 'wikipedia_live'
```
Expected: a number like `120 row(s)` (~one row per active wiki).

```
count 'wikipedia_agg'
```
Expected: a number in the low hundreds (one row per `<wiki, window>` pair).

**Mercel**: "Two minutes ago this was zero. Spark has been streaming
edits into these tables ever since. Let's look at one of them."

```
get 'wikipedia_live', 'enwiki'
```

(Shows the row key `enwiki` with all three column families populated
-- a real recent English Wikipedia edit.)

**Mercel**: "That's the latest edit on English Wikipedia, just now.
Three column families, multiple cells in each. Now let's look at the
windowed aggregations."

```
scan 'wikipedia_agg', {ROWPREFIXFILTER => 'enwiki|', LIMIT => 3}
```

**Mercel**: "Row key is `<wiki>|<reverse-epoch>`. Reverse-epoch means
the newest window sorts first, so 'last N minutes of English
Wikipedia' is a tight prefix scan with zero ORDER BY cost. This is
the textbook HBase row-key design pattern from Lecture 10. That's
Part 3, HBase sink."

### 13:00 — Part 5 BONUS: Spark SQL static-data join

**Alvin** [Terminal C]:

First show the static data file:
```powershell
docker exec -it cs523bdt-lab hdfs dfs -cat /user/static/language_metadata.json | head -3
```

**Alvin**: "There's our static data on HDFS -- an 85-row JSON Lines
file mapping each wiki code to language, continent, region, and
primary country. We hand-authored it. Now we submit the bonus enrich
job, which joins the live stream against this file with a Spark SQL
broadcast join."

```powershell
.\3-run-spark-enrich.bat
```

(That batch file wraps `spark-submit --master yarn --deploy-mode client --executor-memory 768m --num-executors 1 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /opt/project/spark_app/enrich_with_join.py`. The reduced executor memory is required so this app fits alongside the main stream processor in the 8 GB YARN cluster.)

Wait for the `[enrich-wikipedia] join logical plan:` line and the
physical plan that follows it.

**Alvin** (pointing at the printed plan in the terminal):
"Catalyst chose a **BroadcastHashJoin** -- the small 85-row static
side gets shipped to every executor and probed locally. Zero shuffle.
That's the most efficient plan for a stream-static join. This is the
proof we are using the Spark SQL DataFrame API as the rubric asks."

(Wait ~20 seconds for `wikipedia_enriched` to populate, then back in
Terminal E:)

```
scan 'wikipedia_enriched', {LIMIT => 1}
```

**Alvin**: "Each enriched row has the original edit fields PLUS four
new columns from the join -- `ref:language`, `ref:continent`,
`ref:region`, `ref:primary_country`. That's the +2 bonus, end to
end."

### 16:00 — Part 4: Streamlit dashboard

**Mercel** [Terminal D]:

```powershell
.\4-run-dashboard.bat
```

(That batch file wraps `docker exec -it cs523bdt-lab streamlit run /opt/project/dashboard/app.py --server.address 0.0.0.0`. Wait ~10 seconds for the `Local URL: http://localhost:8501` line. If you see a different port like `8502`, run `docker exec cs523bdt-lab pkill -9 -f streamlit` from another terminal, then re-run the batch file -- something else was holding port 8501.)

**Mercel** [browser at `http://localhost:8501`]:

"Here is the dashboard. The whole page refreshes every five seconds."

Walk through top to bottom, briefly:

- **Top of page**: title, plain-language description, and the four
  live metric cards -- edits in the last minute, active wikis,
  most-active wiki, bots vs humans tracked.
- **Top wikis by edits in the last minute** + **Average length delta**
  -- two bar charts side by side. Positive bars in the delta chart
  mean wikis are growing on average; negative means net deletions.
- **Edit types observed** + **Wikipedia namespaces observed** -- two
  more bar charts derived directly from `wikipedia_live`. Edit type
  shows ordinary edits vs new pages vs log entries vs category
  changes; namespace shows Article vs Category vs User and so on.
- **Edits-per-minute trend** -- pick a wiki from the sidebar
  dropdown. The dropdown shows pretty names like "English Wikipedia"
  and "Wikimedia Commons" -- no raw codes anywhere in the UI. Each
  circle on the line is one completed one-minute Spark window.
- **Bonus: continent / language breakdown** -- bar charts of wikis
  grouped by the enrichment join columns, plus a sample-rows table
  showing the joined fields per edit.
- **Bonus: latest edit per language** -- for each language, the most
  recently observed page title and editor. This is the most visibly
  alive part of the page -- the page-title and editor columns shift
  every refresh while the enrich job is running.

(Demonstrate interactivity: switch the sidebar dropdown from "English
Wikipedia" to another wiki such as "Wikimedia Commons" or "French
Wikipedia". The trend chart title changes immediately and the chart
redraws with that wiki's history.)

**Mercel**: "Everything you see on this page is reading from HBase
over Thrift on port 9090, via the `happybase` Python client. The
dashboard auto-refreshes every five seconds, which is why you see
the bar values shift between refreshes."

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
