import pandas as pd
from sqlalchemy import create_engine

from src.common.db import get_target_connection, load_config
from src.common.prerequisite import ensure_schema


PIPELINE_NAME = "pdtester"

SOURCE_SCHEMA = "raw"
SOURCE_TABLE = "pdtester_raw"

TARGET_SCHEMA = "trf"
TARGET_TABLE = "pdtester_trf"


# 🔹 SQLAlchemy engine
def get_engine():
    db_config = load_config()['target_db']

    conn_str = (
        f"mssql+pyodbc://@{db_config['server']}/{db_config['database']}"
        f"?driver={db_config['driver'].replace(' ', '+')}"
        "&trusted_connection=yes"
    )

    return create_engine(conn_str, fast_executemany=True)


# 🔹 Extract from RAW
def extract_data():

    conn = get_target_connection()

    query = f"""
    SELECT *
    FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
    """

    df = pd.read_sql(query, conn)

    print(f"📥 RAW rows: {len(df)}")

    return df


# 🔹 Transform
def transform(df):

    print("🧹 Applying TRF transformations...")

    # ✅ Select only required columns
    keep_cols = [
        "DateTime", "RecordIndex", "TestCage", "Operator",
        "PartNumber", "FixtureNumber", "StepNumber",
        "Station1", "Station2", "Station3"
    ]

    df = df[[col for col in keep_cols if col in df.columns]]

    # ✅ Datetime conversion
    df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")

    # ✅ Clean strings
    if "Operator" in df.columns:
        df["Operator"] = (
            df["Operator"]
            .fillna("")
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
        )

    # ✅ Remove null datetime
    df = df.dropna(subset=["DateTime"])

    # ✅ Dedup (VERY IMPORTANT)
    df = df.sort_values("DateTime").drop_duplicates(
        subset=["RecordIndex"],
        keep="last"
    )

    print(f"✅ After cleaning: {len(df)}")

    return df


# 🔹 Load to TRF
def load_data(df):

    engine = get_engine()

    df.to_sql(
        name=TARGET_TABLE,
        con=engine,
        schema=TARGET_SCHEMA,
        if_exists="replace",   # overwrite
        index=False
    )

    print(f"📤 Loaded into TRF: {len(df)} rows")


# 🔹 MAIN
def run_trf_layer():

    print("🚀 TRF LAYER START → PDTESTER")

    ensure_schema(TARGET_SCHEMA)

    df = extract_data()

    if df.empty:
        print("⚠️ No RAW data")
        return

    df = transform(df)

    load_data(df)

    print("✅ TRF COMPLETED")