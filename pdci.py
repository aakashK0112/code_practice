import pandas as pd
import numpy as np

# -----------------------------
# 1. LOAD DATA
# -----------------------------
# df = pd.read_excel("your_file.xlsx")

# Convert date column
df['TestDate'] = pd.to_datetime(df['TestDate'])


# -----------------------------
# 2. APPLY FILTERS (LIKE USER SELECTION)
# -----------------------------
filtered_df = df[
    (df['Product_Description'] == 'BI_25KV_BUSHING_INSERT') &
    (df['PartNumber'] == '2690557D55') &
    (df['TestCage'] == 'Test#93') &
    (df['Mould_Press'].isin([603, 605])) &
    (df['TestDate'] >= '2026-03-01') &
    (df['TestDate'] <= '2026-03-01')   # <-- change range here
]


# -----------------------------
# 3. PDCI FUNCTION
# -----------------------------
def calculate_pdci(data, value_column):
    
    # Step 1: Remove blanks
    data = data[~data[value_column].isna()]
    
    if len(data) == 0:
        return {
            "Parts_Tested_N": 0,
            "Median_pC_95": np.nan,
            "P95": np.nan,
            "PDCI": np.nan
        }
    
    # Step 2: Parts Tested (Distinct PID)
    N = data['PID_Tested'].nunique()
    
    # Step 3: Get values
    values = data[value_column]
    
    # Step 4: P95
    P95 = np.percentile(values, 95)
    
    # Step 5: Filter <= P95
    filtered_values = values[values <= P95]
    
    # Step 6: Median
    median_pc95 = np.median(filtered_values)
    
    # Step 7: PDCI
    pdci = median_pc95 / P95 if P95 != 0 else np.nan
    
    return {
        "Parts_Tested_N": N,
        "Median_pC_95": round(median_pc95, 2),
        "P95": round(P95, 2),
        "PDCI": round(pdci, 4)
    }


# -----------------------------
# 4. CALCULATE FOR ALL METRICS
# -----------------------------
results = []

metrics = {
    "PD120%": "PD_120",
    "PD100%": "PD_100",
    "PD Locate": "PD_Locate"
}

for metric_name, col in metrics.items():
    res = calculate_pdci(filtered_df, col)
    res["Metric"] = metric_name
    results.append(res)


# -----------------------------
# 5. FINAL TABLE
# -----------------------------
result_df = pd.DataFrame(results)

# Reorder columns
result_df = result_df[
    ["Metric", "Parts_Tested_N", "Median_pC_95", "P95", "PDCI"]
]

print(result_df)