import pandas as pd
import numpy as np

# -----------------------------
# 1. CONFIG
# -----------------------------
USL = 5  # Spec limit


# -----------------------------
# 2. LOAD DATA
# -----------------------------
# Replace with your file path
df = pd.read_excel("your_file.xlsx")

# Convert date column
df['TestDate'] = pd.to_datetime(df['TestDate'])


# -----------------------------
# 3. PDCI FUNCTION
# -----------------------------
def calculate_pdci_advanced(data, value_column):
    
    # Remove blanks
    data = data[~data[value_column].isna()]
    
    if len(data) == 0:
        return {
            "Parts_Tested_N": 0,
            "Median": np.nan,
            "P95": np.nan,
            "PDCI": np.nan
        }
    
    # N (unique PID)
    N = data['PID_Tested'].nunique()
    
    values = data[value_column]
    
    # P95
    P95 = np.percentile(values, 95)
    
    # Filter <= P95
    filtered_values = values[values <= P95]
    
    # Median
    median_val = np.median(filtered_values)
    
    # PDCI (Advanced)
    denominator = (P95 - median_val)
    
    if denominator == 0:
        pdci = np.nan
    else:
        pdci = (USL - median_val) / denominator
    
    return {
        "Parts_Tested_N": N,
        "Median": round(median_val, 2),
        "P95": round(P95, 2),
        "PDCI": round(pdci, 4)
    }


# -----------------------------
# 4. USER FILTER FUNCTION
# -----------------------------
def apply_filters(df, filters):
    
    filtered_df = df.copy()
    
    for col, val in filters.items():
        
        if val is None:
            continue
        
        # List filter (like slicer multi-select)
        if isinstance(val, list):
            filtered_df = filtered_df[filtered_df[col].isin(val)]
        
        # Date range filter
        elif isinstance(val, tuple):
            filtered_df = filtered_df[
                (filtered_df[col] >= val[0]) &
                (filtered_df[col] <= val[1])
            ]
        
        # Single value filter
        else:
            filtered_df = filtered_df[filtered_df[col] == val]
    
    return filtered_df


# -----------------------------
# 5. RUN PDCI FOR ALL METRICS
# -----------------------------
def run_pdci(df, filters):
    
    # Apply filters
    filtered_df = apply_filters(df, filters)
    
    # Metrics mapping
    metrics = {
        "PD120%": "PD_120",
        "PD100%": "PD_100",
        "PD Locate": "PD_Locate"
    }
    
    results = []
    
    for metric_name, col in metrics.items():
        res = calculate_pdci_advanced(filtered_df, col)
        res["Metric"] = metric_name
        results.append(res)
    
    result_df = pd.DataFrame(results)
    
    # Order columns
    result_df = result_df[
        ["Metric", "Parts_Tested_N", "Median", "P95", "PDCI"]
    ]
    
    return result_df


# -----------------------------
# 6. USER INPUT (LIKE SLICERS)
# -----------------------------
filters = {
    "Product_Description": "BI_25KV_BUSHING_INSERT",
    "PartNumber": "2690557D55",
    "TestCage": "Test#93",
    "Mould_Press": [603, 605],  # multi-select
    "TestDate": ("2026-03-01", "2026-03-01")  # date range
}


# -----------------------------
# 7. RUN & OUTPUT
# -----------------------------
output = run_pdci(df, filters)

print(output)