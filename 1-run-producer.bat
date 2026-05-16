@echo off
REM Terminal A -- Wikipedia SSE producer -> Kafka
REM Run this in its own PowerShell window. Press Ctrl+C to stop.
docker exec -it cs523bdt-lab python3 /opt/project/producer/producer.py
