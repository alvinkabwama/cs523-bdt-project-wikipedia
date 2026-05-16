"""Wikipedia EventStreams -> Kafka producer.

Subscribes to https://stream.wikimedia.org/v2/stream/recentchange -- the
Wikimedia Foundation's public Server-Sent Events feed of every edit on
every Wikipedia (and sister project) in real time. Each event is parsed,
trimmed to the fields we care about, and published to Kafka topic
`wikipedia-raw`, keyed by `wiki` so all edits to the same wiki land on
the same partition.

The SSE stream itself runs at roughly 50-200 events per second globally
depending on time of day. We publish each event individually -- no
batching -- so the downstream Spark job sees a true high-throughput
stream rather than the polling-style 10-second buckets used by the
flights and crypto pipelines.

Run inside the lab container:
    docker exec -it cs523bdt-lab python3 /opt/project/producer/producer.py
"""

import json
import logging
import os
import time

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("wikipedia-producer")

SSE_URL = os.environ.get(
    "WIKIPEDIA_SSE_URL",
    "https://stream.wikimedia.org/v2/stream/recentchange",
)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka-server:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "wikipedia-raw")
LOG_EVERY_N = int(os.environ.get("LOG_EVERY_N", "200"))

# Wikimedia's User-Agent policy requires a meaningful, identifying
# User-Agent string with contact info. The default python-requests UA is
# blocked with HTTP 403. See https://meta.wikimedia.org/wiki/User-Agent_policy
USER_AGENT = os.environ.get(
    "WIKIPEDIA_UA",
    "CS523-Bigdata-Student-Project/0.1 (alkaleos10@gmail.com) "
    "kafka-python/2.3 requests/2.34",
)


def transform(raw):
    """Trim a Wikimedia recentchange event down to the fields we keep."""
    length = raw.get("length") or {}
    revision = raw.get("revision") or {}
    return {
        "id":              raw.get("id"),
        "type":            raw.get("type"),
        "wiki":            raw.get("wiki"),
        "title":           raw.get("title"),
        "namespace":       raw.get("namespace"),
        "user":            raw.get("user"),
        "bot":             bool(raw.get("bot", False)),
        "minor":           bool(raw.get("minor", False)),
        "comment":         (raw.get("comment") or "")[:240],
        "timestamp":       raw.get("timestamp"),
        "server_url":      raw.get("server_url"),
        "server_name":     raw.get("server_name"),
        "length_old":      length.get("old"),
        "length_new":      length.get("new"),
        "length_delta": (
            (length.get("new") or 0) - (length.get("old") or 0)
            if length.get("new") is not None and length.get("old") is not None
            else None
        ),
        "revision_old":    revision.get("old"),
        "revision_new":    revision.get("new"),
        "ingestion_ts":    int(time.time()),
    }


def build_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=1,
        linger_ms=20,
        retries=3,
        max_in_flight_requests_per_connection=1,
    )


def stream_sse_events(url):
    """Yield parsed JSON events from a Wikimedia EventStreams endpoint.

    The endpoint speaks Server-Sent Events. Each event is two lines:
        event: message
        data: {json}
    Events are separated by a blank line. We need to keep the underlying
    HTTP connection open and parse line-by-line as bytes arrive.
    """
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": USER_AGENT,
    }
    while True:
        try:
            with requests.get(url, stream=True, headers=headers, timeout=60) as r:
                r.raise_for_status()
                for raw_line in r.iter_lines(decode_unicode=True):
                    if not raw_line:
                        continue
                    if raw_line.startswith("data: "):
                        payload = raw_line[len("data: "):]
                        try:
                            yield json.loads(payload)
                        except json.JSONDecodeError:
                            continue
        except (requests.RequestException, ConnectionError) as e:
            log.warning("SSE connection dropped: %s; reconnecting in 5s", e)
            time.sleep(5)
        except Exception as e:
            log.error("unexpected error in SSE loop: %s", e)
            time.sleep(5)


def main():
    log.info("starting: bootstrap=%s topic=%s sse=%s",
             KAFKA_BOOTSTRAP, TOPIC, SSE_URL)
    producer = build_producer()
    sent = 0
    start = time.time()
    for raw_event in stream_sse_events(SSE_URL):
        try:
            rec = transform(raw_event)
            wiki = rec.get("wiki")
            if not wiki:
                continue
            producer.send(TOPIC, key=wiki, value=rec)
            sent += 1
            if sent % LOG_EVERY_N == 0:
                rate = sent / (time.time() - start) if (time.time() - start) else 0
                log.info("sent=%d  rate=%.1f events/sec", sent, rate)
        except KafkaError as exc:
            log.error("send failed: %s", exc)


if __name__ == "__main__":
    main()
