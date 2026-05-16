@echo off
REM Terminal B -- Spark Structured Streaming (3 queries) on YARN
REM Run this in its own PowerShell window AFTER Terminal A is publishing.
REM Wait until you see "state: RUNNING" before opening Terminal C.
docker exec -it cs523bdt-lab spark-submit --master yarn --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /opt/project/spark_app/stream_processor.py
