# CS523 Big Data Technologies — Final Project (Wikipedia variant) — TEAM NOTES

**This file is for Alvin and Mercel.** Do NOT commit your personal exam
notes here. The public-facing README is `README.md`; if anyone clones
the repo, that is what they see by default.

---

## Team
- Alvin Leonald Kabwama
- Mercel Vubangsi

## Rubric coverage (target: 10 + 2 = 12 / 12)

| Part | Pts | Implementation |
|---|---|---|
| 1. Kafka ingestion | 3 | `producer/producer.py` — SSE subscriber, publishes one JSON message per edit, keyed by `wiki` |
| 2. Spark Structured Streaming | 3 | `spark_app/stream_processor.py` — three concurrent queries with watermark + checkpoint |
| 3. HBase sink | 2 | `wikipedia_live` + `wikipedia_agg` (reverse-epoch row key) via `foreachBatch` |
| 4. Visualisation dashboard | 2 | `dashboard/app.py` — Streamlit, metric cards + bar charts + trend + bonus continent breakdown |
| 5. **Bonus** Spark SQL static join | +2 | `spark_app/enrich_with_join.py` — broadcast join with `language_metadata.json` on HDFS |

## Pipeline summary

```
Wikimedia SSE  ──►  Kafka wikipedia-raw  ──►  Spark on YARN  ──►  HBase wikipedia_*  ──►  Streamlit
```

## Publishing to GitHub

```bash
git init -b main
git add .
git commit -m "Initial commit: CS523 final project (Wikipedia stream pipeline)"
gh repo create cs523-bdt-project-wikipedia --public --source=. --push
```

## Demo recording

- ≤20 minutes on Microsoft Streams
- Both teammates on camera the entire time
- Both teammates speaking
- Walkthrough script: `docs/demo-script.md`

## Caveats / known limitations

- **Wikimedia User-Agent policy**: Wikimedia rejects requests with the
  default `python-requests/*` User-Agent (HTTP 403). The producer sends
  an identifying UA that includes the team's contact email:
  `CS523-Bigdata-Student-Project/0.1 (alkaleos10@gmail.com) ...`. Do not
  remove it. Wikimedia's policy is at
  https://meta.wikimedia.org/wiki/User-Agent_policy.
- **YARN headroom**: 8 GB total. The main stream_processor uses ~5 GB by
  default; enrich must be launched with `--executor-memory 768m
  --num-executors 1` (~2 GB) to fit. If both apps are in `ACCEPTED` state,
  kill one and resubmit with smaller resources.
- **Wikimedia SSE reconnects**: the producer automatically reconnects
  every 5 seconds on connection drop, but does NOT replay events it
  missed during the gap. This matches how real-world push-streaming
  consumers behave.
- **HBase + ZooKeeper across container restarts**: if HMaster comes up
  with `PleaseHoldException`, run
  `docker exec zookeeper-server bash -c "echo deleteall /hbase | zkCli.sh -server localhost:2181"`
  then restart HBase. This is in the demo-script backup plan.
- **Unknown wikis in the bonus join**: the static lookup
  (`data/static/language_metadata.json`) covers ~85 major wikis. Less
  active wikis appear in HBase with `(unknown)` in the
  language/continent/region columns. The dashboard filters those out
  of the bar charts but keeps them in the sample-rows table so the
  LEFT JOIN behaviour is still visible. The chart x-axis labels are
  still readable for these unmatched wikis because of the smart
  fallback in `pretty_wiki()` — see below.

## What the dashboard does to keep raw wiki codes out of the user-facing UI

Three layers, in priority order:

1. **`WIKI_DISPLAY_NAMES`** (curated dict at the top of `dashboard/app.py`)
   maps about 90 specific wiki codes (`enwiki`, `commonswiki`,
   `wikidatawiki`, `zh_yuewiki`, …) to bespoke names.
2. **`LANGUAGE_CODE_TO_NAME` + `PROJECT_SUFFIX_TO_NAME`** (smart fallback)
   parses any unknown code as `<lang><project>` and combines the two
   halves. Covers ~150 ISO-639 language codes and 8 project suffixes.
   Example: `frwikibooks` → "French Wikibooks", `ckbwiki` → "Central
   Kurdish (Sorani) Wikipedia".
3. **Raw code as last resort.** If both lookups miss, `pretty_wiki()`
   returns the original code so the bar is still labelled — but in our
   experience this is rare.

The **sidebar trend-wiki picker** is now a **dropdown**, not a text
input. Labels are pretty names; the underlying wiki code is resolved
internally via a reverse-lookup dict (`pretty_to_code`). The user never
needs to know about `enwiki`-style codes.

## Screenshots for the README

Permanent doc assets live in `docs/screenshots/`. The main `README.md`
embeds these by relative path. **Do not delete or rename them** —
GitHub renders the README with these images inline.

| File | What it shows |
|---|---|
| `docs/screenshots/01-overview.png` | Page header + description + four metric cards |
| `docs/screenshots/02-top-wikis-and-delta.png` | Top wikis by edit count + average length delta charts |
| `docs/screenshots/03-edit-types-namespaces.png` | Edit-type and namespace breakdown charts |
| `docs/screenshots/04-trend.png` | Edits-per-minute trend line (selected wiki) |
| `docs/screenshots/05-bonus-continent.png` | Bonus continent / language breakdown + sample-rows table |
| `docs/screenshots/06-bonus-latest-edits.png` | Bonus "latest edit per language" live table |

To refresh these (after a UI tweak), run the dashboard, open it in a
browser, and re-capture each section with the same names.

## Demo backup plans

| Symptom | Fix |
|---|---|
| `HMaster PleaseHoldException` | Clear ZK `/hbase` znode + restart HBase |
| Producer SSE drops | Wait 5 seconds; auto-reconnect kicks in |
| YARN refuses 2nd Spark app | Kill stream_processor before submitting enrich |
| Dashboard shows stale data | Confirm row counts climbing via `hbase shell` |
