import pandas as pd
from sqlalchemy import create_engine

from src.common.db import get_target_connection
from src.common.prerequisite import ensure_schema
from src.common.config_loader import get_pipeline_config


# 🔹 PIPELINE NAME
PIPELINE_NAME = "qrstickers"

# 🔹 LOAD CONFIG
config = get_pipeline_config(PIPELINE_NAME)

BRONZE_SCHEMA = config["bronze"]["schema"]
BRONZE_TABLE = config["bronze"]["table"]

SILVER_SCHEMA = config["silver"]["schema"]
SILVER_TABLE = config["silver"]["table"]


# 🔥 EXTRACT
def extract_data():

    conn = get_target_connection()

    query = f"""
    SELECT *
    FROM {BRONZE_SCHEMA}.{BRONZE_TABLE}
    """

    df = pd.read_sql(query, conn)

    print(f"📥 Extracted {len(df)} rows from BRONZE")

    return df


# 🔥 TRANSFORM (YOUR NOTEBOOK LOGIC)
def transform(df):

    print("🔄 Transform started...")

    # =========================================================
    # STEP 1: Select Required Columns (SAFE)
    # Future-proof: handles new columns automatically
    # =========================================================

    required_cols = [
        "TrcID"
    ]

    # Keep only columns that exist
    cols_to_use = [c for c in required_cols if c in df.columns]

    df = df[cols_to_use].copy()

    print(f"📌 Columns selected: {cols_to_use}")

    # =========================================================
    # STEP 1: Ensure TrcID is string
    # =========================================================
    df["TrcID"] = df["TrcID"].astype(str)


    # =========================================================
    # STEP 2: Extract Press
    # Example: 60326020401533801 → 603
    # =========================================================
    df["QRsticker_Press"] = df["TrcID"].str[:3]


    # =========================================================
    # STEP 3: Extract Date (YYMMDD → Date)
    # =========================================================
    df["QRsticker_Date"] = pd.to_datetime(
        df["TrcID"].str[3:9],
        format="%y%m%d",
        errors="coerce"
    )


    # =========================================================
    # STEP 4: Extract Time (HHMMSS → Time)
    # =========================================================
    df["QRsticker_Time"] = pd.to_datetime(
        df["TrcID"].str[9:15],
        format="%H%M%S",
        errors="coerce"
    ).dt.time


    # =========================================================
    # STEP 5: Extract Cavity
    # =========================================================
    df["QRsticker_cavity"] = df["TrcID"].str[15:18]


    # =========================================================
    # STEP 6: Create Sticker ID (unique group)
    # =========================================================
    df["QRsticker_id"] = df["TrcID"].str[:15]


    # =========================================================
    # STEP 7: Create Timestamp
    # =========================================================
    df["QRsticker_Timestamp"] = pd.to_datetime(
        df["QRsticker_id"],
        format="%y%m%d%H%M%S",
        errors="coerce"
    )


    # =========================================================
    # STEP 8: Data Cleaning
    # =========================================================
    df["QRsticker_Press"] = pd.to_numeric(df["QRsticker_Press"], errors="coerce")

    df["QRsticker_cavity"] = (
        df["QRsticker_cavity"]
        .astype(str)
        .str.zfill(2)
    )


    # =========================================================
    # STEP 9: Group → One row per sticker
    # =========================================================
    grouped = (
        df.groupby("QRsticker_id", as_index=False)
        .agg({
            "QRsticker_Press": "first",
            "QRsticker_Timestamp": "min",
            "QRsticker_Date": "min",
            "QRsticker_Time": "min",
            "QRsticker_cavity": lambda x: sorted(x.unique())
        })
    )


    # =========================================================
    # STEP 10: Define Max Cavity per Press
    # =========================================================
    press_max_cavity = {
        603: 12,
        605: 12,
        925: 16
    }


    # =========================================================
    # STEP 11: Calculate Missing Cavities
    # =========================================================
    def calculate_missing(row):

        press = row["QRsticker_Press"]
        cavities_printed = set(row["QRsticker_cavity"])

        max_cav = press_max_cavity.get(press)

        if not max_cav:
            return pd.Series([None, None])

        expected = {str(i).zfill(2) for i in range(1, max_cav + 1)}

        missing = sorted(expected - cavities_printed)

        return pd.Series([
            ",".join(missing),
            len(missing)
        ])


    grouped[
        ["QRsticker_cavity_not_printed", "Visually_Rejected_Parts"]
    ] = grouped.apply(calculate_missing, axis=1)


    # =========================================================
    # STEP 12: Final Columns Selection
    # =========================================================
    final_df = grouped[
        [
            "QRsticker_id",
            "QRsticker_Press",
            "QRsticker_Timestamp",
            "QRsticker_Date",
            "QRsticker_Time",
            "QRsticker_cavity_not_printed",
            "Visually_Rejected_Parts"
        ]
    ]


    print("✅ Transform completed")

    return final_df

# 🔥 LOAD
def load_data(df):

    conn = get_target_connection()
    engine = create_engine("mssql+pyodbc://", creator=lambda: conn)

    df.to_sql(
        SILVER_TABLE,
        engine,
        schema=SILVER_SCHEMA,
        if_exists="append",
        index=False
    )

    print(f"📤 Loaded {len(df)} rows into {SILVER_SCHEMA}.{SILVER_TABLE}")


# 🔥 MAIN
def run_trf_layer():

    print("🚀 STARTING TRF → QRSTICKERS")

    # ✅ ensure schema exists
    ensure_schema(SILVER_SCHEMA)

    df = extract_data()

    if df.empty:
        print("⚠️ No data found")
        return

    df = transform(df)

    load_data(df)

    print("✅ TRF QRSTICKERS COMPLETED")
    
    
    


from src.layers.raw.qr_stickers import run_raw_layer
from src.layers.trf.qr_stickers import run_trf_layer

def run_qr_pipeline():

    print("🚀 STARTING QR PIPELINE")

    run_raw_layer()
    run_trf_layer()

    print("✅ QR PIPELINE COMPLETED")


if __name__ == "__main__":
    run_qr_pipeline()