import socket
import uuid
from datetime import datetime
import pyodbc


# =========================
# DB CONNECTION
# =========================
def get_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=YOUR_SERVER;"
        "DATABASE=YOUR_DB;"
        "UID=YOUR_USER;"
        "PWD=YOUR_PASSWORD"
    )


# =========================
# ENSURE SCHEMA + TABLE
# =========================
def ensure_audit_table():
    conn = get_connection()
    cursor = conn.cursor()

    # Create schema if not exists
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'audit')
        BEGIN
            EXEC('CREATE SCHEMA audit')
        END
    """)

    # Create table if not exists
    cursor.execute("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'audit' 
            AND TABLE_NAME = 'job_run'
        )
        BEGIN
            CREATE TABLE audit.job_run (
                job_audit_id INT IDENTITY(1,1) PRIMARY KEY,
                job_run_id VARCHAR(50),
                job_name VARCHAR(100),
                host_name VARCHAR(100),
                triggered_by VARCHAR(100),
                start_time DATETIME,
                end_time DATETIME,
                status VARCHAR(20),
                total_duration_seconds INT,
                total_duration_minutes FLOAT,
                error_message VARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
        END
    """)

    conn.commit()
    conn.close()


# =========================
# START JOB
# =========================
def start_job(job_name, triggered_by="system"):
    conn = get_connection()
    cursor = conn.cursor()

    job_run_id = str(uuid.uuid4())
    host_name = socket.gethostname()
    start_time = datetime.now()

    cursor.execute("""
        INSERT INTO audit.job_run (
            job_run_id, job_name, host_name, triggered_by, start_time, status
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, job_run_id, job_name, host_name, triggered_by, start_time, "RUNNING")

    conn.commit()
    conn.close()

    return job_run_id, start_time


# =========================
# END JOB
# =========================
def end_job(job_run_id, start_time, status, error_message=None):
    conn = get_connection()
    cursor = conn.cursor()

    end_time = datetime.now()
    duration_seconds = int((end_time - start_time).total_seconds())
    duration_minutes = duration_seconds / 60

    cursor.execute("""
        UPDATE audit.job_run
        
        SET 
            end_time = ?,
            status = ?,
            total_duration_seconds = ?,
            total_duration_minutes = ?,
            error_message = ?
        WHERE job_run_id = ?
    """, end_time, status, duration_seconds, duration_minutes, error_message, job_run_id)

    conn.commit()
    conn.close()
    
    
#main.py
from layers.audit.job_audit import (
    ensure_audit_table,
    start_job,
    end_job
)

# 👉 your actual pipeline
from pipelines.press_pipeline import run_press_pipeline


def main():

    print("=" * 60)
    print("JOB STARTED")
    print("=" * 60)

    # ✅ Step 1: Ensure audit infra
    ensure_audit_table()

    # ✅ Step 2: Start job audit
    job_run_id, job_start_time = start_job("Press_Pipeline_Job")

    job_status = "SUCCESS"
    error_msg = None

    try:
        # 🔥 Run your pipeline
        run_press_pipeline()

    except Exception as e:
        job_status = "FAILED"
        error_msg = str(e)

        print("❌ ERROR:", error_msg)

        # important → rethrow for visibility
        raise

    finally:
        # ✅ Step 3: End job audit
        end_job(
            job_run_id=job_run_id,
            start_time=job_start_time,
            status=job_status,
            error_message=error_msg
        )

        print("=" * 60)
        print(f"JOB STATUS: {job_status}")
        print("=" * 60)


if __name__ == "__main__":
    main()
    
    
1. ensure_audit_table()
   → creates schema/table if missing

2. start_job()
   → inserts row (status = RUNNING)

3. run_press_pipeline()

4. end_job()
   → updates:
      - status (SUCCESS / FAILED)
      - end_time
      - duration
      - error_message (if any)