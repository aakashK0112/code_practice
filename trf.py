import pandas as pd
from sqlalchemy import create_engine

from src.common.db import get_target_connection
from src.common.prerequisite import ensure_schema
from src.common.config_loader import get_pipeline_config

# 🔹 PIPELINE NAME
PIPELINE_NAME = "press"

# 🔹 LOAD CONFIG
config = get_pipeline_config(PIPELINE_NAME)

RAW_SCHEMA = "raw"
RAW_TABLE = "press_raw"

TRF_SCHEMA = "trf"
TRF_TABLE = "press_trf"


# 🔥 EXTRACT FROM RAW
def extract_data():

    conn = get_target_connection()

    query = f"""
    SELECT *
    FROM {RAW_SCHEMA}.{RAW_TABLE}
    """

    df = pd.read_sql(query, conn)

    print(f"📥 Extracted {len(df)} rows from RAW")

    return df


# 🔥 TRANSFORM LOGIC (FROM YOUR NOTEBOOK)
def transform(df):

    print("🔄 Transform started...")

    # =========================================================
    # STEP 1: Select Required Columns
    # =========================================================
    # Only keep columns needed for analysis / dashboard
    keep_cols = [
        "Press", "RecordIndex", "DateTime", "StepNumber",
        "FunctionDescription", "PartNumber", "RunStart",
        "CycleEnd", "CycleTime", "Operator",
        "TopPlate", "BottomPlate", "Oven1", "Oven2",
        "TCUA", "TCUB", "TCUC", "TCUD",
        "ScrewRubber", "NozzleRubber", "ScrewJacket",
        "InjectJacket", "InjectPosition", "ClampPosition",
        "ScrewRPM", "InjectRate", "InjectPressure",
        "TopEjectorPosition", "EnergyMonitor",
        "SystemPressure", "SystemFlow",
        "InjectValvePosition", "AuxHeat"
    ]

    # Keep only existing columns (safe)
    cols_to_use = [c for c in keep_cols if c in df.columns]
    df = df[cols_to_use].copy()

    print(f"✅ Columns selected: {len(cols_to_use)}")


    # =========================================================
    # STEP 2: Clean Text Columns (FunctionDescription)
    # =========================================================
    if "FunctionDescription" in df.columns:
        df["FunctionDescription"] = (
            df["FunctionDescription"]
            .fillna("")                # handle null
            .str.strip()              # remove spaces
            .str.replace(r"\s+", " ", regex=True)  # remove multiple spaces
            .str.title()              # standard format
        )

    print("✅ FunctionDescription cleaned")


    # =========================================================
    # STEP 3: Clean Text Columns (Operator)
    # =========================================================
    if "Operator" in df.columns:
        df["Operator"] = (
            df["Operator"]
            .fillna("")
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
            .str.title()
        )

    print("✅ Operator cleaned")


    # =========================================================
    # STEP 4: Convert Date Columns
    # =========================================================
    if "DateTime" in df.columns:
        df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")

    if "RunStart" in df.columns:
        df["RunStart"] = pd.to_datetime(df["RunStart"], errors="coerce")

    if "CycleEnd" in df.columns:
        df["CycleEnd"] = pd.to_datetime(df["CycleEnd"], errors="coerce")

    print("✅ Date columns converted")


    # =========================================================
    # STEP 5: Sort Data Properly
    # =========================================================
    df = df.sort_values(
        by=["Press", "RecordIndex", "DateTime"],
        ascending=[True, True, True]
    ).reset_index(drop=True)

    print("✅ Data sorted")


    # =========================================================
    # STEP 6: Handle Missing / Invalid Values (Optional)
    # =========================================================
    # Example: fill numeric nulls if needed
    # df["CycleTime"] = df["CycleTime"].fillna(0)

    print("✅ Missing values handled (if applicable)")


    # =========================================================
    # STEP 7: Add Derived Columns (Future Use)
    # =========================================================
    # Example (you can enable later):
    # df["Shift"] = df["DateTime"].dt.hour.apply(lambda x: "Day" if x < 12 else "Night")

    print("✅ Derived columns ready (if needed)")


    print("🎯 Transform completed successfully")

    return df

# 🔥 LOAD TO TRF
def load_data(df):

    conn = get_target_connection()
    engine = create_engine("mssql+pyodbc://", creator=lambda: conn)

    df.to_sql(
        TRF_TABLE,
        engine,
        schema=TRF_SCHEMA,
        if_exists="append",
        index=False
    )

    print(f"📤 Loaded {len(df)} rows into {TRF_SCHEMA}.{TRF_TABLE}")


# 🔥 MAIN TRF RUNNER
def run_trf_layer():

    print("🚀 STARTING TRF → PRESS")

    # ✅ IMPORTANT FIX (your issue)
    ensure_schema(TRF_SCHEMA)

    df = extract_data()

    if df.empty:
        print("⚠️ No data found in RAW")
        return

    df = transform(df)

    load_data(df)

    print("✅ TRF PRESS COMPLETED")









from src.layers.raw.press import run_raw_layer
from src.layers.trf.press import run_trf_layer

def run_press_pipeline():

    print("🚀 STARTING PRESS PIPELINE")

    # RAW
    run_raw_layer()

    # TRF
    run_trf_layer()

    print("✅ PRESS PIPELINE COMPLETED")


if __name__ == "__main__":
    run_press_pipeline()