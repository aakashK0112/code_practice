Pipeline starts
   ↓
ensure_schema("mart")
   ↓
ensure_audit_table()
   ↓
RAW → TRF → MART
   ↓
log_audit()



from src.common.audit import ensure_audit_table

def run_all():

    # 🔥 Create audit table automatically
    ensure_audit_table()

    # then run pipelines
    run_press_pipeline()
    run_qr_pipeline()
    
    
audit.py

from datetime import datetime
from src.common.db import get_target_connection


# 🔥 STEP 1: Ensure audit table exists
def ensure_audit_table():

    conn = get_target_connection()
    cursor = conn.cursor()

    cursor.execute("""
    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'mart'
        AND TABLE_NAME = 'pipeline_audit'
    )
    BEGIN
        CREATE TABLE mart.pipeline_audit (
            audit_id INT IDENTITY(1,1) PRIMARY KEY,
            pipeline_name VARCHAR(50),
            layer VARCHAR(20),
            start_time DATETIME,
            end_time DATETIME,
            duration_sec FLOAT,
            rows_extracted INT,
            rows_loaded INT,
            status VARCHAR(20),
            error_message VARCHAR(MAX)
        )
    END
    """)

    conn.commit()
    cursor.close()
    conn.close()


# 🔥 STEP 2: Log audit
def log_audit(
    pipeline_name,
    layer,
    start_time,
    end_time,
    rows_extracted,
    rows_loaded,
    status,
    error_message=None
):

    conn = get_target_connection()
    cursor = conn.cursor()

    duration = (end_time - start_time).total_seconds()

    cursor.execute("""
        INSERT INTO mart.pipeline_audit (
            pipeline_name,
            layer,
            start_time,
            end_time,
            duration_sec,
            rows_extracted,
            rows_loaded,
            status,
            error_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        pipeline_name,
        layer,
        start_time,
        end_time,
        duration,
        rows_extracted,
        rows_loaded,
        status,
        error_message
    ))

    conn.commit()
    cursor.close()
    conn.close()
    
SPC_TOOL/
│
├── config/
│   ├── db_config.yaml
│   ├── pipeline_config.yaml
│   ├── limits_config.yaml   ✅ (HERE)
│
├── src/
│   ├── common/
│   │   ├── db.py
│   │   ├── audit.py         ✅
│   │   ├── config_loader.py
│   │   ├── prerequisite.py
│   │   ├── load_limits.py   ✅
│
│   ├── layers/
│   │   ├── raw/
│   │   ├── trf/
│   │   ├── mart/
│
│   ├── runners/
│   │   ├── run_all.py
│   │   ├── run_press.py
│
├── .env                     ✅ (passwords)
├── requirements.txt


import yaml
from datetime import datetime
from src.common.db import get_target_connection


CONFIG_PATH = "config/limits_config.yaml"


def load_yaml():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def deactivate_existing(cursor, table_name):
    """
    Deactivate currently active records
    """
    cursor.execute(f"""
        UPDATE {table_name}
        SET is_active = 0,
            effective_to = GETDATE()
        WHERE is_active = 1
    """)


def insert_pyrometer_limits(cursor, data, effective_date):
    """
    Insert pyrometer limits
    """
    for row in data:
        cursor.execute("""
            INSERT INTO mart.pyrometer_limits
            (parameter, lower_limit, upper_limit, target, effective_from, effective_to, is_active)
            VALUES (?, ?, ?, ?, ?, NULL, 1)
        """, (
            row["parameter"],
            row["lower"],
            row["upper"],
            row["target"],
            effective_date
        ))


def insert_material_limits(cursor, data, effective_date):
    """
    Insert material limits
    """
    for row in data:
        cursor.execute("""
            INSERT INTO mart.material_limits
            (parameter, lower_limit, upper_limit, effective_from, effective_to, is_active)
            VALUES (?, ?, ?, ?, NULL, 1)
        """, (
            row["parameter"],
            row["lower"],
            row["upper"],
            effective_date
        ))


def load_limits():
    print("🚀 Starting LIMITS LOAD...")

    config = load_yaml()
    conn = get_target_connection()
    cursor = conn.cursor()

    effective_date = datetime.today().date()

    try:
        # 🔹 PYROMETER LIMITS
        if "pyrometer" in config:
            print("🔄 Updating Pyrometer Limits...")

            deactivate_existing(cursor, "mart.pyrometer_limits")
            insert_pyrometer_limits(cursor, config["pyrometer"], effective_date)

            print("✅ Pyrometer Limits Updated")

        # 🔹 MATERIAL LIMITS
        if "material" in config:
            print("🔄 Updating Material Limits...")

            deactivate_existing(cursor, "mart.material_limits")
            insert_material_limits(cursor, config["material"], effective_date)

            print("✅ Material Limits Updated")

        conn.commit()
        print("🎉 LIMITS LOAD COMPLETED SUCCESSFULLY")

    except Exception as e:
        conn.rollback()
        print("❌ LIMIT LOAD FAILED:", str(e))
        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    load_limits()
    

# =========================================
# 🔥 PYROMETER LIMITS
# =========================================
pyrometer:
  - parameter: "Top Platen"
    lower: 320
    upper: 350
    target: 330

  - parameter: "Aux1"
    lower: 310
    upper: 340
    target: 325

  - parameter: "Aux2"
    lower: 315
    upper: 345
    target: 330

  - parameter: "Bottom Platen"
    lower: 300
    upper: 330
    target: 315


# =========================================
# 🔥 MATERIAL LIMITS
# =========================================
material:
  - parameter: "ML"
    lower: 0.28
    upper: 0.40

  - parameter: "MH"
    lower: 5.18
    upper: 8.92

  - parameter: "TS2"
    lower: 0.98
    upper: 1.36

  - parameter: "TC90"
    lower: 3.47
    upper: 4.19

  - parameter: "MOONEY"
    lower: 7
    upper: 27

  - parameter: "TENSILE"
    lower: 709
    upper: 1153

  - parameter: "ELONG"
    lower: 490
    upper: 1432

  - parameter: "M25"
    lower: 76
    upper: 112

  - parameter: "M50"
    lower: 102
    upper: 156

  - parameter: "M75"
    lower: 109
    upper: 199

  - parameter: "M100"
    lower: 108
    upper: 252

  - parameter: "M200"
    lower: 124
    upper: 496

  - parameter: "M300"
    lower: 198
    upper: 666

  - parameter: "TEAR C"
    lower: 123
    upper: 153

  - parameter: "DURO"
    lower: 43
    upper: 55

  - parameter: "SG"
    lower: 1.276
    upper: 1.324

  - parameter: "CAP"
    lower: 263.42
    upper: 286.16

  - parameter: "DF"
    lower: 0.15
    upper: 0.34

  - parameter: "DS"
    lower: 925
    upper: 1129

  - parameter: "DC"
    lower: 2.4
    upper: 2.7
    
    
from datetime import datetime
from src.common.db import get_target_connection


# 🔥 STEP 1: Ensure audit table exists
def ensure_audit_table():

    conn = get_target_connection()
    cursor = conn.cursor()

    cursor.execute("""
    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'mart'
        AND TABLE_NAME = 'pipeline_audit'
    )
    BEGIN
        CREATE TABLE mart.pipeline_audit (
            audit_id INT IDENTITY(1,1) PRIMARY KEY,
            pipeline_name VARCHAR(50),
            layer VARCHAR(20),
            start_time DATETIME,
            end_time DATETIME,
            duration_sec FLOAT,
            rows_extracted INT,
            rows_loaded INT,
            status VARCHAR(20),
            error_message VARCHAR(MAX)
        )
    END
    """)

    conn.commit()
    cursor.close()
    conn.close()


# 🔥 STEP 2: Log audit
def log_audit(
    pipeline_name,
    layer,
    start_time,
    end_time,
    rows_extracted,
    rows_loaded,
    status,
    error_message=None
):

    conn = get_target_connection()
    cursor = conn.cursor()

    duration = (end_time - start_time).total_seconds()

    cursor.execute("""
        INSERT INTO mart.pipeline_audit (
            pipeline_name,
            layer,
            start_time,
            end_time,
            duration_sec,
            rows_extracted,
            rows_loaded,
            status,
            error_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        pipeline_name,
        layer,
        start_time,
        end_time,
        duration,
        rows_extracted,
        rows_loaded,
        status,
        error_message
    ))

    conn.commit()
    cursor.close()
    conn.close()