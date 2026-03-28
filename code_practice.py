import pandas as pd
from datetime import datetime, timedelta

from sqlalchemy import create_engine

from src.common.db import get_source_connection, load_config
from src.common.prerequisite import ensure_schema
from src.common.config_loader import get_pipeline_config


# 🔹 PIPELINE NAME
PIPELINE_NAME = "pdtester"

# 🔹 LOAD CONFIG
config = get_pipeline_config(PIPELINE_NAME)

TARGET_SCHEMA = config["target"]["schema"]
TARGET_TABLE = config["target"]["table"]

SQL_TEMPLATE = config["query"]

BUFFER_HOURS = config["load"]["buffer_hours"]
INITIAL_START = config["load"]["initial_start_date"]


# 🔹 Create SQLAlchemy engine
def get_engine():

    db_config = load_config()['target_db']

    connection_string = (
        f"mssql+pyodbc://@{db_config['server']}/{db_config['database']}"
        f"?driver={db_config['driver'].replace(' ', '+')}"
        "&trusted_connection=yes"
    )

    return create_engine(connection_string, fast_executemany=True)


# 🔹 Check table exists
def table_exists():

    from src.common.db import get_target_connection

    conn = get_target_connection()
    cursor = conn.cursor()

    cursor.execute(f"""
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{TARGET_SCHEMA}'
    AND TABLE_NAME = '{TARGET_TABLE}'
    """)

    return cursor.fetchone()[0] > 0


# 🔹 Get last timestamp
def get_last_timestamp():

    from src.common.db import get_target_connection

    conn = get_target_connection()
    cursor = conn.cursor()

    cursor.execute(f"""
        SELECT MAX(DateTime)
        FROM {TARGET_SCHEMA}.{TARGET_TABLE}
    """)

    return cursor.fetchone()[0]


# 🔹 Build query
def build_query(start_date):
    return SQL_TEMPLATE.format(start_date=start_date)


# 🔹 Extract data
def extract_data():

    conn = get_source_connection()

    if not table_exists():
        print("🟢 FIRST RUN")

        start_date = INITIAL_START
        load_type = "FULL"

    else:
        last_time = get_last_timestamp()

        start_date = (
            last_time - timedelta(hours=BUFFER_HOURS)
        ).strftime("%Y-%m-%d %H:%M:%S")

        print(f"🔵 INCREMENTAL FROM {start_date}")

        load_type = "INCREMENTAL"

    query = build_query(start_date)

    df = pd.read_sql(query, conn)

    print(f"📥 Extracted {len(df)} rows")

    return df, load_type


# 🔹 Add metadata columns
def add_metadata(df, load_type):

    df["load_timestamp"] = datetime.now()
    df["source_name"] = PIPELINE_NAME
    df["load_type"] = load_type

    return df


# 🔹 Load data
def load_data(df):

    if df.empty:
        print("⚠️ No data to load")
        return

    engine = get_engine()

    df.to_sql(
        name=TARGET_TABLE,
        con=engine,
        schema=TARGET_SCHEMA,
        if_exists="append",
        index=False,
        method="multi"
    )

    print(f"📤 Loaded {len(df)} rows into {TARGET_SCHEMA}.{TARGET_TABLE}")


# 🔹 MAIN
def run_raw_layer():

    print("🚀 RAW LAYER START → PDTESTER (WITH METADATA)")

    # Ensure schema
    ensure_schema(TARGET_SCHEMA)

    df, load_type = extract_data()

    if df.empty:
        print("⚠️ No data extracted")
        return

    # Add metadata columns
    df = add_metadata(df, load_type)

    # Load
    load_data(df)

    print("✅ RAW LAYER COMPLETED")