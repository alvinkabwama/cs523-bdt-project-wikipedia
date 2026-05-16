@echo off
REM Terminal C -- Bonus Spark SQL static-data join (+2 pts)
REM Run this AFTER Terminal B is RUNNING. The reduced executor memory
REM (--executor-memory 768m --num-executors 1) is required so both Spark
REM applications fit inside the 8 GB YARN cluster.
docker exec -it cs523bdt-lab spark-submit --master yarn --deploy-mode client --executor-memory 768m --num-executors 1 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /opt/project/spark_app/enrich_with_join.py
