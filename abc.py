from src.common.pipeline import run_pipeline
from src.common.db import extract_table, load_table
from src.common.metrics import generate_fpy

# =========================================
# CONFIG
# =========================================
TRF_SCHEMA = "trf"
TRF_TABLE = "pdtester_trf"

MART_SCHEMA = "mart"
MART_TABLE = "pdtester_fpy_kpis"


# =========================================
# EXTRACT
# =========================================
def extract():
    return extract_table(TRF_SCHEMA, TRF_TABLE)


# =========================================
# TRANSFORM (KPI LOGIC)
# =========================================
def transform(df):
    df = df.copy()

    # Step 1: Ensure datatype consistency
    if "PID_Tested" in df.columns:
        df["PID_Tested"] = df["PID_Tested"].astype(str)

    # Step 2: Generate FPY (COMMON LOGIC)
    df = generate_fpy(
        df,
        keys=[
            "Product_Description",
            "TestCase",
            "PartNumber",
            "Mould_Press",
            "Mould_Date",
            "TestDate",
            "ShiftDate",
            "Shift"
        ]
    )

    # Step 3: Select required columns (clean output)
    final_cols = [
        "Product_Description",
        "TestCase",
        "PartNumber",
        "Mould_Press",
        "Mould_Date",
        "TestDate",
        "ShiftDate",
        "Shift",
        "Total_Parts_Tested",
        "Unique_Parts_Tested",
        "FPY_Total_N",
        "FPY_Group_N",
        "FPY_Individual_N",
        "FPY_Total_P",
        "FPY_Group_P",
        "FPY_Individual_P"
    ]

    df = df[final_cols]

    return df


# =========================================
# LOAD
# =========================================
def load(df):
    load_table(df, MART_SCHEMA, MART_TABLE)


# =========================================
# RUN
# =========================================
if __name__ == "__main__":
    run_pipeline(extract, transform, load, "FPY KPIs")
    
    
    
    
    
    
from src.common.pipeline import run_pipeline
from src.common.db import extract_table, load_table
from src.common.metrics import generate_fpy
import numpy as np

# =========================================
# CONFIG
# =========================================
TRF_SCHEMA = "trf"
TRF_TABLE = "pdtester_trf"

MART_SCHEMA = "mart"
MART_TABLE = "pdtester_fpy_shift"


# =========================================
# EXTRACT
# =========================================
def extract():
    return extract_table(TRF_SCHEMA, TRF_TABLE)


# =========================================
# TRANSFORM (SHIFT LOGIC ONLY)
# =========================================
def transform(df):
    df = df.copy()

    # Step 1: Ensure datatype consistency
    if "PID_Tested" in df.columns:
        df["PID_Tested"] = df["PID_Tested"].astype(str)

    # Step 2: Generate FPY (COMMON LOGIC)
    df = generate_fpy(
        df,
        keys=[
            "Product_Description",
            "TestCase",
            "PartNumber",
            "Mould_Press",
            "Mould_Date",
            "TestDate",
            "ShiftDate",
            "Shift"
        ]
    )

    # Step 3: Create Shift Groups
    df["Shift_Group"] = np.where(
        df["Shift"].isin(["A", "Morning"]),
        "Morning",
        "Evening"
    )

    # Step 4: Aggregate shift-wise FPY
    df_shift = (
        df.groupby([
            "Product_Description",
            "TestCase",
            "PartNumber",
            "Mould_Press",
            "ShiftDate"
        ])
        .apply(lambda x: pd.Series({
            "FPY_Morning": x.loc[x["Shift_Group"] == "Morning", "FPY_Group_N"].mean(),
            "FPY_Evening": x.loc[x["Shift_Group"] == "Evening", "FPY_Group_N"].mean()
        }))
        .reset_index()
    )

    # Step 5: Handle nulls
    df_shift["FPY_Morning"] = df_shift["FPY_Morning"].fillna(0)
    df_shift["FPY_Evening"] = df_shift["FPY_Evening"].fillna(0)

    return df_shift


# =========================================
# LOAD
# =========================================
def load(df):
    load_table(df, MART_SCHEMA, MART_TABLE)


# =========================================
# RUN
# =========================================
if __name__ == "__main__":
    run_pipeline(extract, transform, load, "FPY SHIFT KPI")