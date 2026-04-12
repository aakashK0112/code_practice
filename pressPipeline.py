src/
│
├── pipelines/
│   └── press_pipeline.py   ✅ (NEW)
│
├── layers/
│   ├── raw/
│   │   └── press.py        ✅ (MODIFIED)
│   │
│   ├── trf/
│   │   └── press.py        ✅ (MODIFIED)
│   │
│   └── mart/
│       ├── press_parameter_var.py  ✅ (MODIFIED)
│       └── other_marts.py          (future)
│
├── common/
│   ├── db.py
│   ├── config_loader.py
│   └── prerequisite.py
│
└── main.py


src/layers/raw/press.py

import pandas as pd
from datetime import datetime, timedelta
from src.common.db import get_connection
from src.common.config_loader import get_pipeline_config

PIPELINE_NAME = "press"
config = get_pipeline_config(PIPELINE_NAME)

SQL_TEMPLATE = config["query"]
BUFFER_HOURS = config["load"]["buffer_hours"]
INITIAL_START = config["load"]["initial_start_date"]


def get_last_timestamp(conn, schema, table):
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(DateTime) FROM {schema}.{table}")
    result = cursor.fetchone()[0]
    return result


def extract_data():
    print("📥 RAW: Extract started")

    conn = get_connection("source")

    # incremental logic
    last_time = get_last_timestamp(conn, config["target"]["schema"], config["target"]["table"])

    if last_time is None:
        start_date = INITIAL_START
        load_type = "FULL"
    else:
        start_date = (last_time - timedelta(hours=BUFFER_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
        load_type = "INCREMENTAL"

    query = SQL_TEMPLATE.format(start_date=start_date)

    df = pd.read_sql(query, conn)

    print(f"✅ RAW Extracted: {len(df)} rows")

    return df, load_type


src/layers/trf/press.py

import pandas as pd

def transform(df):

    print("🔄 TRF: Transform started")

    keep_cols = [
        "RecordIndex", "DateTime", "StepNumber", "FunctionDescription",
        "PartNumber", "RunStart", "CycleTime", "Operator",
        "Location", "TopPlaten", "TopMold", "Aux1", "Aux2",
        "BottomMold", "BottomPlaten", "InjectPressure"
    ]

    cols_to_use = [c for c in keep_cols if c in df.columns]
    df = df[cols_to_use].copy()

    # Clean text columns
    if "FunctionDescription" in df.columns:
        df["FunctionDescription"] = (
            df["FunctionDescription"]
            .fillna("")
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
            .str.title()
        )

    if "Location" in df.columns:
        df["Location"] = df["Location"].astype(str).str.replace(r"\d", "", regex=True)

    print("✅ TRF completed")

    return df


src/layers/mart/press_parameter_var.py

import pandas as pd

def transform(df):

    print("📊 MART: press_parameter_var started")

    df = df.copy()

    df["Press_Date"] = pd.to_datetime(df["RunStart"]).dt.date

    df = df[
        [
            "Press_Date",
            "Location",
            "RecordIndex",
            "Aux1",
            "Aux2",
            "TopMold",
            "InjectPressure",
            "TopPlaten",
            "BottomMold",
            "BottomPlaten",
        ]
    ]

    df["Time_Min"] = (
        df.groupby(["Press_Date", "Location", "RecordIndex"])
        .cumcount()
    )

    print("✅ MART completed")

    return df

src/pipelines/press_pipeline.py

from concurrent.futures import ThreadPoolExecutor

from src.layers.raw.press import extract_data
from src.layers.trf.press import transform as trf_transform
from src.layers.mart.press_parameter_var import transform as mart_param_var


def run_press_pipeline():

    print("🚀 PRESS PIPELINE STARTED")

    # STEP 1: RAW
    raw_df, load_type = extract_data()

    if raw_df.empty:
        print("⚠️ No data found")
        return

    # STEP 2: TRF
    trf_df = trf_transform(raw_df)

    # STEP 3: MARTS (PARALLEL)
    mart_functions = {
        "press_parameter_var": mart_param_var,
        # add more marts here
    }

    mart_results = {}

    def run_mart(name, func):
        print(f"⚙️ Running {name}")
        return name, func(trf_df)

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(run_mart, name, func)
                   for name, func in mart_functions.items()]

        for future in futures:
            name, result = future.result()
            mart_results[name] = result

    print("✅ PRESS PIPELINE COMPLETED")

    return mart_results

main.py

from src.pipelines.press_pipeline import run_press_pipeline

if __name__ == "__main__":
    run_press_pipeline()