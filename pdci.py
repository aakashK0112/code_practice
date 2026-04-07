import pandas as pd
import numpy as np

USL = 5

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
    
    # NEW FORMULA
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