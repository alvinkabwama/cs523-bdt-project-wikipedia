#!/usr/bin/env bash
# One-shot setup for the WIKIPEDIA pipeline. Idempotent -- safe to re-run.
#
# Run from the host AFTER `docker compose up -d`:
#     ./setup_container.sh

set -e
LAB=cs523bdt-lab
run() { docker exec "$LAB" bash -c "$1"; }

echo "[setup-wikipedia] 1/7  installing Python deps..."
run "apt-get update -q >/dev/null && apt-get install -y -q python3-pip >/dev/null"
run "pip3 install --break-system-packages -q kafka-python requests happybase streamlit pandas altair"

echo "[setup-wikipedia] 2/7  starting YARN..."
run "jps | grep -q ResourceManager || /opt/hadoop/sbin/start-yarn.sh >/dev/null 2>&1"

echo "[setup-wikipedia] 3/7  starting HBase + Thrift server..."
run "jps | grep -q HMaster        || /opt/hbase/bin/start-hbase.sh         >/dev/null 2>&1"
run "jps | grep -q ThriftServer   || /opt/hbase/bin/hbase-daemon.sh start thrift >/dev/null 2>&1"

echo "[setup-wikipedia] 4/7  waiting for HBase Master..."
run "
  until echo \"create '_setup_ready_', 'cf'\" | hbase shell 2>&1 | grep -q 'Created table'; do
    sleep 5
  done
  echo \"disable '_setup_ready_'; drop '_setup_ready_'\" | hbase shell >/dev/null 2>&1
"

echo "[setup-wikipedia] 5/7  ensuring Kafka topic wikipedia-raw exists..."
docker exec kafka-server bash -c "
  kafka-topics --bootstrap-server kafka-server:9092 --list 2>/dev/null | grep -q '^wikipedia-raw$' \
    || kafka-topics --bootstrap-server kafka-server:9092 --create --topic wikipedia-raw --partitions 1 --replication-factor 1
"

echo "[setup-wikipedia] 6/7  creating HBase tables (idempotent)..."
run "
  existing=\$(echo 'list' | hbase shell 2>/dev/null | grep -Eo 'wikipedia_(live|agg|enriched)')
  for t in wikipedia_live wikipedia_agg wikipedia_enriched; do
    if ! echo \"\$existing\" | grep -q \"^\$t\$\"; then
      case \$t in
        wikipedia_live)     echo \"create 'wikipedia_live', {NAME => 'info', VERSIONS => 1}, {NAME => 'stats', VERSIONS => 1}, {NAME => 'meta', VERSIONS => 1}\" ;;
        wikipedia_agg)      echo \"create 'wikipedia_agg', {NAME => 'm', VERSIONS => 1}\" ;;
        wikipedia_enriched) echo \"create 'wikipedia_enriched', {NAME => 'info', VERSIONS => 1}, {NAME => 'stats', VERSIONS => 1}, {NAME => 'ref', VERSIONS => 1}\" ;;
      esac | hbase shell >/dev/null 2>&1
      echo \"  created \$t\"
    else
      echo \"  \$t exists -- skipping\"
    fi
  done
"

echo "[setup-wikipedia] 7/7  uploading static lookup JSON to HDFS..."
run "hdfs dfs -mkdir -p /user/static /user/spark/checkpoints"
run "hdfs dfs -test -e /user/static/language_metadata.json \
     || hdfs dfs -put /opt/project/data/static/language_metadata.json /user/static/language_metadata.json"

echo
echo "[setup-wikipedia] done. Run the pipeline in 4 terminals:"
echo
echo "  Terminal A: docker exec -it $LAB python3 /opt/project/producer/producer.py"
echo "  Terminal B: docker exec -it $LAB spark-submit --master yarn --deploy-mode client \\"
echo "                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \\"
echo "                /opt/project/spark_app/stream_processor.py"
echo "  Terminal C: docker exec -it $LAB spark-submit --master yarn --deploy-mode client \\"
echo "                --executor-memory 768m --num-executors 1 \\"
echo "                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \\"
echo "                /opt/project/spark_app/enrich_with_join.py"
echo "  Terminal D: docker exec -it $LAB streamlit run /opt/project/dashboard/app.py --server.address 0.0.0.0"
echo
echo "  Dashboard: http://localhost:8501"
