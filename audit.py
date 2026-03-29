import pandas as pd
from sqlalchemy import create_engine

from src.common.db import get_target_connection
from src.common.prerequisite import ensure_schema


# =========================================================
# 🔁 YOUR REUSABLE FUNCTIONS (MOVE TO COMMON LATER)
# =========================================================

def generate_fpy(df, group_cols):

    df = df.copy()

    # Sort for stable attempt calculation
    df = df.sort_values(group_cols + ['PID_Tested'])

    # Attempt number
    df['Attempt_No'] = df.groupby(group_cols + ['PID_Tested']).cumcount() + 1

    result_list = []

    for keys, df_group in df.groupby(group_cols, dropna=False):

        if not isinstance(keys, tuple):
            keys = (keys,)

        row = {col: val for col, val in zip(group_cols, keys)}

        row['Total_Parts_Tested'] = len(df_group)
        row['Unique_Parts_Tested'] = df_group['PID_Tested'].nunique()

        max_attempt = int(df_group['Attempt_No'].max())

        for i in range(1, max_attempt + 1):

            temp = df_group[df_group['Attempt_No'] == i]

            row[f'TC{i}_N'] = temp['PID_Tested'].nunique()

            row[f'TC{i}_PD120'] = (temp['PD120_Pass'] == 'Pass').sum()
            row[f'TC{i}_PD100'] = (temp['PD100_Pass'] == 'Pass').sum()
            row[f'TC{i}_PDLocate'] = (temp['PDLocate_Pass'] == 'Pass').sum()

            row[f'TC{i}_to_TC{i+1}'] = (temp['PDLocate_Failed'] == 'Failed').sum()

        result_list.append(row)

    return pd.DataFrame(result_list)


def calculate_fpy(df):

    df = df.copy()

    df['FPY_N'] = df['TC1_PD120'] + df['TC1_PD100']

    df['TPY_N'] = (
        df['TC1_PD120'] +
        df['TC1_PD100'] +
        df['TC1_PDLocate']
    )

    df['FPY'] = df['FPY_N'] / df['Unique_Parts_Tested']
    df['TPY'] = df['TPY_N'] / df['Unique_Parts_Tested']

    df['FPY_Percent'] = df['FPY'] * 100

    return df


# =========================================================
# 📥 EXTRACT
# =========================================================

def extract_data():

    conn = get_target_connection()

    df = pd.read_sql("""
        SELECT *
        FROM trf.pdtester_trf
    """, conn)

    return df


# =========================================================
# 🔄 TRANSFORM (UPDATED)
# =========================================================

def transform(df):

    print("🔄 Applying FPY logic...")

    # =========================================
    # STEP 1: REQUIRED COLUMNS (SAFE)
    # =========================================
    required_cols = [
        "Product_Description",
        "TestCage",
        "PartNumber",
        "Mould_Press",
        "ShiftDate",
        "Shift",
        "PID_Tested",
        "PD120_Pass",
        "PD100_Pass",
        "PDLocate_Pass",
        "PDLocate_Failed"
    ]

    cols = [c for c in required_cols if c in df.columns]
    df = df[cols].copy()

    print(f"✅ Columns used: {cols}")

    # =========================================
    # STEP 2: APPLY YOUR FPY FUNCTION
    # =========================================
    group_cols = [
        "Product_Description",
        "TestCage",
        "PartNumber",
        "Mould_Press",
        "ShiftDate",
        "Shift"
    ]

    df_fpy = generate_fpy(df, group_cols)

    # =========================================
    # STEP 3: CALCULATE KPI
    # =========================================
    df_final = calculate_fpy(df_fpy)

    return df_final


# =========================================================
# 📤 LOAD
# =========================================================

def load_data(df):

    conn = get_target_connection()
    engine = create_engine("mssql+pyodbc://", creator=lambda: conn)

    df.to_sql(
        "pdtester_shift_kpi",
        engine,
        schema="mart",
        if_exists="replace",
        index=False
    )

    print("✅ Loaded to mart.pdtester_shift_kpi")


# =========================================================
# 🚀 RUN
# =========================================================

def run_mart_layer():

    print("🚀 START MART PDTESTER")

    ensure_schema("mart")

    df = extract_data()

    df_final = transform(df)

    load_data(df_final)

    print("✅ MART PDTESTER DONE")