@echo off
REM Terminal D -- Streamlit dashboard
REM Run this LAST. Then open http://localhost:8501 in your browser.
docker exec -it cs523bdt-lab streamlit run /opt/project/dashboard/app.py --server.address 0.0.0.0
