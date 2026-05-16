# Demo Recording Script — Wikipedia Edit Stream Pipeline

**Target duration**: 20 minutes (hard cap).
**Format**: Microsoft Teams meeting with both teammates on camera, screen
share on whoever is driving. Record the Teams meeting; upload to
Microsoft Streams; share link with the professor.

**Rubric requirements**:
- 20 minutes or less.
- Both teammates visible on camera the whole time (split-screen webcams).
- Both teammates actively speak.
- Must show data flowing end-to-end: source -> Kafka -> Spark -> HBase -> dashboard.
- Cover all five rubric parts in order: 1) Kafka, 2) Spark Streaming,
  3) HBase, 4) Streamlit dashboard, 5) Bonus Spark SQL static-data join.

---

## Pre-flight checklist (do 15 minutes before recording)

These commands are **NOT part of the recorded demo**. They prepare the
environment so that when the camera starts rolling, the lab stack is
healthy and no Spark applications are running yet.

Run all of these in one PowerShell window, in the repo directory:

```powershell
cd "C:\Users\user\Documents\MIU DE Courses\Big Data\cs523-bdt-project-wikipedia"

# 1. Confirm Docker Desktop is running
docker version

# 2. Bring up the lab stack (4 containers: zookeeper, kafka, postgres, cs523bdt-lab)
docker compose up -d
docker ps --format "table {{.Names}}`t{{.Status}}"
# Expect: all four containers in state "Up"

# 3. Run the one-shot setup script (installs Python deps, starts YARN +
#    HBase + Thrift, creates the Kafka topic and the 3 HBase tables,
#    uploads the static JSON metadata to HDFS). Idempotent -- safe to re-run.
bash .\setup_container.sh

# 4. Confirm Wikimedia EventStreams is reachable from inside the container
docker exec cs523bdt-lab python3 -c "import requests; r = requests.get('https://stream.wikimedia.org/v2/stream/recentchange', stream=True, headers={'User-Agent': 'CS523-preflight'}, timeout=5); print('SSE status:', r.status_code); r.close()"
# Expect: SSE status: 200

# 5. Confirm no Spark applications are still running from earlier runs
curl -s "http://localhost:8088/ws/v1/cluster/apps?states=RUNNING"
# Expect: {"apps":null} or {"apps":{}}

# 6. Kill any leftover streamlit / producer processes from prior runs
docker exec cs523bdt-lab pkill -9 -f streamlit
docker exec cs523bdt-lab pkill -9 -f "producer.py"

# 7. Open these tabs in your browser, but do NOT switch to them until on camera:
#    - http://localhost:8088   (YARN ResourceManager UI)
#    - http://localhost:16010  (HBase Master UI)
#    - http://localhost:8501   (Streamlit dashboard -- will 404 until we run Part 4)
#    - https://stream.wikimedia.org/v2/stream/recentchange  (the raw SSE feed)

# 8. Arrange 4 empty PowerShell windows in a 2x2 grid, all already
#    cd'd into the repo directory. These become Terminals A, B, C, D
#    during the demo.
```

### About the `.bat` helper files

Inside the repo root there are four batch files. Each one wraps a single
`docker exec -it` command so PowerShell does not split it across lines
(which would otherwise mangle the `--packages` URL and fail). All four
pipeline commands in this script are run via these wrappers:

| File | What it runs | Run in |
|---|---|---|
| `1-run-producer.bat`     | Wikimedia SSE producer -> Kafka         | Terminal A |
| `2-run-spark-stream.bat` | Spark Structured Streaming on YARN      | Terminal B |
| `3-run-spark-enrich.bat` | Spark SQL bonus enrichment join on YARN | Terminal C |
| `4-run-dashboard.bat`    | Streamlit dashboard                     | Terminal D |

Each file is a single line. Press `Ctrl+C` in any of them to stop that
component. The lab container itself keeps running.

---

## Minute-by-minute outline

The five rubric parts are presented **in order** (1 -> 5). Part 4
(dashboard) is launched before Part 5 (bonus enrichment) so that when
the bonus job starts, we can simply refresh the dashboard to watch the
bonus charts come alive — that gives a strong visual payoff to close
the demo.

| Time | Section | Who speaks | What's on screen |
|---:|---|---|---|
| 0:00 – 1:00   | Intro                                       | Both    | Webcams full-frame |
| 1:00 – 3:00   | Architecture overview                       | Alvin   | `docs/architecture.md` ASCII diagram |
| 3:00 – 4:00   | Codebase tour                               | Mercel  | File tree in VS Code / explorer |
| 4:00 – 5:30   | Data source: Wikimedia EventStreams (SSE)   | Mercel  | Raw SSE feed in a browser tab |
| 5:30 – 7:30   | Part 1 — Kafka producer                     | Alvin   | Terminal A — `.\1-run-producer.bat` |
| 7:30 – 10:30  | Part 2 — Spark Structured Streaming         | Both    | Terminal B + YARN UI at `:8088` |
| 10:30 – 12:30 | Part 3 — HBase sink                         | Mercel  | `hbase shell` showing populated tables |
| 12:30 – 15:00 | Part 4 — Streamlit dashboard                | Mercel  | Terminal D + browser at `:8501` |
| 15:00 – 18:00 | Part 5 — Bonus Spark SQL static-data join   | Alvin   | Terminal C + dashboard bonus charts |
| 18:00 – 19:00 | What we learned                             | Both    | Faces full-frame |
| 19:00 – 20:00 | Wrap                                        | Both    | Repo URL + thanks |

---

## Detailed script

Each section below follows the same format:

- **Who & where** — who speaks and which terminal / browser / tab is on screen.
- **Commands to run** — copy-pasteable, in the order they go.
- **What to say** — the spoken lines, in quotes.
- **What you should see** — the visible result, so you know when to move on.

---

### 0:00 — Intro

**Who & where**: Both teammates, webcams full-frame, no screen share.

**What to say**:

- **Alvin**: "Hi, I'm Alvin Leonald Kabwama."
- **Mercel**: "And I'm Mercel Vubangsi."
- **Alvin**: "This is our CS523 Big Data Technologies final project — a
  real-time analytics pipeline for the Wikipedia edit stream. Total run
  time: under twenty minutes."

---

### 1:00 — Architecture overview

**Who & where**: Alvin speaks, screen shares `docs/architecture.md` with
the ASCII diagram visible.

**What to say** (Alvin):

- "Our source is Wikimedia EventStreams — every edit on every Wikipedia
  in real time, pushed to us over **Server-Sent Events**. This is true
  streaming, not REST polling. Roughly 100 events per second worldwide."
- "Our producer is a Python script that keeps an open HTTP/2 connection
  to Wikimedia, parses each event, and publishes one Kafka message per
  edit, keyed by the wiki code."
- "On the consumption side, a PySpark Structured Streaming application
  running on YARN subscribes to the Kafka topic and runs three queries:
  a live snapshot per wiki, a per-wiki edit count over 1-minute tumbling
  windows, and a per-wiki average length delta over the same window."
- "All three queries write to HBase — one wide-column row per wiki for
  the live snapshot, and one row per (wiki, window) for the aggregations."
- "For the +2 bonus, a second Spark application joins each incoming
  event against a hand-authored language metadata file on HDFS — wiki
  code maps to language name, continent, region, and primary country."
- "The dashboard reads HBase over Thrift and auto-refreshes every five
  seconds."

---

### 3:00 — Codebase tour

**Who & where**: Mercel speaks, screen shares the repo file tree (either
the VS Code Explorer pane open at the repo root, or a `tree` / `ls`
output in a terminal — whichever is easier to read on camera).

**What to say** (Mercel):

- "Here is the repository. We deliberately kept it flat and small —
  under 500 lines of Python across the whole pipeline. The three code
  folders are `producer/`, `spark_app/`, and `dashboard/`. `producer/`
  contains one file, `producer.py`, which reads the Wikimedia SSE feed
  and publishes to Kafka. `spark_app/` has two PySpark jobs:
  `stream_processor.py`, the main streaming application with the three
  queries Alvin described, and `enrich_with_join.py`, the bonus Spark
  SQL join. `dashboard/` has `app.py`, the Streamlit app. Under
  `data/static/` is the 85-row JSON Lines file with our language
  metadata, which gets uploaded to HDFS at setup time."
- "At the root you can see `docker-compose.yml` for the four lab
  containers, `setup_container.sh` for the one-time bootstrap, and the
  four numbered `.bat` files we'll launch in this demo — one per
  pipeline component. The `docs/` folder has the architecture diagram,
  this script, and the screenshots in the README. Everything you'll see
  on screen for the next sixteen minutes comes from those few files —
  no hidden infrastructure, no managed services. Just open-source
  components glued together with a small amount of Python."

**What you should see**: a tidy file tree where the viewer can
visually count the small number of folders and files involved.

---

### 4:00 — Data source: Wikimedia EventStreams

**Who & where**: Mercel speaks, screen shares
`https://stream.wikimedia.org/v2/stream/recentchange` in a browser tab.

**What to say** (Mercel):

- "This is the raw Server-Sent Events feed. Every line that starts with
  `data:` is one JSON-encoded edit event. You can see them streaming
  past in real time — these are Wikipedia edits happening right now, all
  over the world, in every language."
- "Wikimedia operates this feed publicly and for free. No API key, no
  rate limit. It's the canonical streaming source the Wikimedia
  Foundation uses to teach this exact pattern."

**What you should see**: a continuous wall of `data:` lines scrolling
past in the browser tab.

---

### 5:30 — Part 1: Kafka producer

**Who & where**: Alvin speaks, screen shares Terminal A.

**Command to run** (Alvin, in Terminal A):

```powershell
.\1-run-producer.bat
```

(This wraps `docker exec -it cs523bdt-lab python3 /opt/project/producer/producer.py`.)

**What to say** (Alvin, while waiting ~15 seconds for first log line):

- "Our producer is subscribing to the Wikimedia Server-Sent Events feed,
  parsing each event, trimming it to the fields we care about, and
  publishing one Kafka message per event, keyed by the wiki code."
- "You can see the rate — typically 50 to 200 events per second
  worldwide depending on the time of day. Every line you see is one
  real Wikipedia edit that just happened somewhere in the world."

**What you should see**: log lines like
`sent=200 rate=87.4 events/sec` printing every few seconds.

**Optional** (only if time allows — skip if running long): peek into
Kafka directly to prove messages landed.

```powershell
docker exec -it kafka-server kafka-console-consumer --bootstrap-server kafka-server:9092 --topic wikipedia-raw --max-messages 1 --timeout-ms 5000
```

- **Alvin**: "There's one real edit pulled straight out of the Kafka
  topic — wiki, title, user, namespace, byte delta. That is Part 1,
  Kafka ingestion."

---

### 7:30 — Part 2: Spark Structured Streaming

**Who & where**: Mercel runs the command in Terminal B; Alvin then takes
the mic to walk through the YARN UI and the Spark UI.

**Command to run** (Mercel, in Terminal B):

```powershell
.\2-run-spark-stream.bat
```

(This wraps
`spark-submit --master yarn --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /opt/project/spark_app/stream_processor.py`.)

**What you should see**: Spark logs scroll for ~45 seconds, then a YARN
state transition from `ACCEPTED` to `RUNNING`.

While Spark is starting, switch to the YARN UI in the browser at
`http://localhost:8088`.

**What to say** (Alvin, looking at the YARN UI):

- "There's our application — `WikipediaEditsProcessor`, state RUNNING.
  YARN allocated three containers: one ApplicationMaster and two
  executors, totaling about 5 GB of memory."

Switch to the Spark UI at `http://localhost:4040`.

**What to say** (Alvin, looking at the Spark UI):

- "In the Spark UI we can see our three concurrent streaming queries —
  live snapshot per wiki, per-wiki edit count over 1-minute tumbling
  windows, and per-wiki average byte delta over the same window. Each
  one has its own HDFS checkpoint directory for exactly-once semantics
  across restarts."

**What to say** (Mercel):

- "All three queries are watermarked at 30 seconds — we promise Spark
  no event arrives more than 30 seconds late, which lets Structured
  Streaming close window state and free memory. Without a watermark, the
  windowed aggregations would grow state forever."

---

### 10:30 — Part 3: HBase sink

**Who & where**: Mercel speaks. Open `hbase shell` in any free
PowerShell window (the producer and Spark stream don't need to be
visible right now).

**Command to run** (Mercel):

```powershell
docker exec -it cs523bdt-lab hbase shell
```

Wait for the `hbase(main):001:0>` prompt, then run:

```
count 'wikipedia_live'
```

**What you should see**: a number roughly equal to the number of wikis
edited in the last minute — typically 80 to 150 rows.

**What to say** (Mercel):

- "`wikipedia_live` has one row per wiki — the latest edit on that wiki.
  Spark overwrites the row whenever a new event for that wiki arrives."

```
get 'wikipedia_live', 'enwiki'
```

**What you should see**: all three column families
(`info:`, `stats:`, `meta:`) populated with values from a real recent
English Wikipedia edit.

**What to say** (Mercel):

- "Three column families: `info` for the identifier fields, `stats` for
  the byte-length numbers, `meta` for timestamps and flags. HBase stores
  each family in its own set of HFiles, which is why denormalizing by
  access pattern is the right design here."

```
scan 'wikipedia_agg', {ROWPREFIXFILTER => 'enwiki|', LIMIT => 3}
```

**What you should see**: three rows whose keys start with `enwiki|` and
contain windowed metrics.

**What to say** (Mercel):

- "Row key here is `<wiki>|<reverse-epoch>`. Reverse-epoch means the
  newest window sorts first, so 'last N minutes of English Wikipedia'
  becomes a tight prefix scan with zero ORDER BY cost. That is the
  textbook HBase row-key design pattern from Lecture 10."
- "That is Part 3, HBase sink, working end to end."

Exit the HBase shell:
```
exit
```

---

### 12:30 — Part 4: Streamlit dashboard

**Who & where**: Mercel runs the command in Terminal D, then drives the
browser walkthrough.

**Command to run** (Mercel, in Terminal D):

```powershell
.\4-run-dashboard.bat
```

(This wraps
`docker exec -it cs523bdt-lab streamlit run /opt/project/dashboard/app.py --server.address 0.0.0.0`.)

**What you should see**: a `Local URL: http://localhost:8501` line
within ~10 seconds. If you see `8502` or another port, run
`docker exec cs523bdt-lab pkill -9 -f streamlit` from another terminal
and retry — something was still holding port 8501.

Open `http://localhost:8501` in the browser.

**What to say** (Mercel, walking the page top to bottom):

- "Here is the dashboard. The whole page refreshes every five seconds."
- **Top of page** — "Title and a plain-language description, then four
  live metric cards: edits in the last minute, active wikis, the
  most-active wiki right now, and the bots-vs-humans ratio of editors
  we've captured."
- **Top wikis + Average length delta** — "Two bar charts side by side.
  Positive bars in the delta chart mean wikis are growing on average;
  negative means net deletions."
- **Edit types + Wikipedia namespaces** — "Two more bar charts derived
  directly from `wikipedia_live`. Edit type shows ordinary edits versus
  new pages versus log entries versus category changes; namespace shows
  Article versus Category versus User and so on."
- **Edits-per-minute trend** — "Pick a wiki from the sidebar dropdown.
  Notice the dropdown shows pretty names like 'English Wikipedia' and
  'Wikimedia Commons' — no raw codes anywhere in the UI. Each circle on
  the line is one completed one-minute Spark window."
- (Demonstrate interactivity: switch the dropdown from "English
  Wikipedia" to another option such as "Wikimedia Commons" or "French
  Wikipedia". The trend chart redraws with that wiki's history.)
- **Bonus sections at the bottom** — "There are two bonus charts down
  here — a continent / language breakdown, and the latest edit per
  language. Right now they are empty because we have not started the
  bonus job yet. We will turn that on next, then come back to this page
  and watch them populate live."
- "Everything you see on this page is reading from HBase over Thrift on
  port 9090, via the `happybase` Python client. That is Part 4."

---

### 15:00 — Part 5: Bonus Spark SQL static-data join

**Who & where**: Alvin runs the command in Terminal C. Mercel keeps the
dashboard tab visible alongside (or comes back to it at the end of the
section).

**Step 1** — show the static data file. Alvin, in any free terminal:

```powershell
docker exec -it cs523bdt-lab hdfs dfs -cat /user/static/language_metadata.json | head -3
```

**What you should see**: three JSON lines, each one mapping a wiki code
to language, continent, region, and primary country.

**What to say** (Alvin):

- "There's our static reference data on HDFS — an 85-row JSON Lines
  file we hand-authored. Now we submit the enrich job, which joins the
  live stream against this file with a Spark SQL broadcast join."

**Step 2** — submit the enrich job. Alvin, in Terminal C:

```powershell
.\3-run-spark-enrich.bat
```

(This wraps
`spark-submit --master yarn --deploy-mode client --executor-memory 768m --num-executors 1 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /opt/project/spark_app/enrich_with_join.py`.
The reduced executor memory is required so this second app fits
alongside the main stream processor in the 8 GB YARN cluster.)

Wait for the `[enrich-wikipedia] join logical plan:` line and the
physical plan that follows it.

**What to say** (Alvin, pointing at the printed plan):

- "Catalyst chose a **BroadcastHashJoin** — the small 85-row static
  side gets shipped to every executor and probed locally. Zero shuffle.
  That's the most efficient plan for a stream-static join. This is the
  proof we are using the Spark SQL DataFrame API as the rubric asks."

**Step 3** — wait ~20 seconds for `wikipedia_enriched` to populate, then
switch back to the dashboard browser tab.

**What to say** (Mercel, on the dashboard):

- "Watch the bottom of the page — the bonus charts that were empty a
  moment ago are now filling in. That continent / language breakdown is
  computed from the join columns the enrich job is writing. And below
  it, 'latest edit per language' — that table refreshes every five
  seconds with the most recent observed edit per language."

**Step 4** (optional, if there's time) — show the enriched HBase rows
directly:

```powershell
docker exec cs523bdt-lab bash -c "echo \"scan 'wikipedia_enriched', {LIMIT => 1}\" | hbase shell 2>/dev/null"
```

**What to say** (Alvin):

- "Each enriched row carries the original edit fields PLUS four new
  columns from the join — `ref:language`, `ref:continent`,
  `ref:region`, `ref:primary_country`. That's the +2 bonus, end to end."

---

### 18:00 — What we learned

**Who & where**: Both teammates, webcams full-frame.

**What to say**:

- **Alvin**: "The biggest mindset shift for us was leaning into HBase's
  wide-column model instead of fighting it. In a relational database we
  would have normalized the live snapshot and the windowed aggregates
  into separate tables and joined them on wiki code. In HBase we kept
  them in two separate tables but designed each row key around the
  question being asked of it — the wiki code alone for live lookups,
  and `<wiki>|<reverse-epoch>` for window scans. Two tables, two access
  patterns, zero joins on the read path. That made the dashboard
  queries trivially fast."
- **Mercel**: "What surprised me most about Spark Structured Streaming
  is that the code is almost identical to the batch DataFrame API we
  used in earlier labs. Same `groupBy(window(...))` syntax, same SQL
  functions. The streaming part is essentially a couple of extra knobs
  — a watermark to keep windowed state from growing forever, and a
  trigger to control output cadence. Once we had the schema right, the
  streaming application looked just like a batch query we already knew
  how to write — which is exactly the unification Spark is selling."

---

### 19:00 — Wrap

**Who & where**: Both teammates, webcams full-frame.

**What to say** (both):

- "The repo is at `github.com/alvinkabwama/cs523-bdt-project-wikipedia`.
  Reproducing this demo is: `docker compose up -d`, then
  `./setup_container.sh`, then the four `.bat` files in numerical order.
  Thanks for watching."

---

## Recovery — what to do if something goes wrong mid-recording

| Symptom | Quick fix |
|---|---|
| `1-run-producer.bat` exits with HTTP 403 | The User-Agent header is wrong. Check `producer/producer.py` line ~30. |
| `2-run-spark-stream.bat` stuck on `ACCEPTED` for > 90 s | YARN has stale containers. Stop the app from the YARN UI and re-run. |
| `3-run-spark-enrich.bat` fails with `cannot allocate container` | The main stream is using all of YARN. The `--executor-memory 768m --num-executors 1` flags inside the `.bat` should prevent this; if it still fails, restart the lab container. |
| Dashboard opens on port 8502 not 8501 | An orphan Streamlit is holding 8501. Run `docker exec cs523bdt-lab pkill -9 -f streamlit` and re-run `.\4-run-dashboard.bat`. |
| `hbase shell` says `PleaseHoldException` | HBase did not finish starting. Wait ~30 s and retry. If it persists, run `docker exec zookeeper-server bash -c "echo deleteall /hbase | zkCli.sh -server localhost:2181"`, then `bash .\setup_container.sh` again. |
| Dashboard bonus sections stay empty after Part 5 | The enrich job did not write any rows yet. Wait another 30 s and refresh. Confirm Terminal C is still running. |
