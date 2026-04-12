Cp =
VAR USL = MAX('limits'[USL])
VAR LSL = MAX('limits'[LSL])
VAR Sigma =
    CALCULATE(
        STDEV.P('fact_table'[Value])
    )
RETURN
    DIVIDE(USL - LSL, 6 * Sigma)
    
    
Cpk =
VAR USL = MAX('limits'[USL])
VAR LSL = MAX('limits'[LSL])
VAR MeanVal =
    CALCULATE(AVERAGE('fact_table'[Value]))
VAR Sigma =
    CALCULATE(STDEV.P('fact_table'[Value]))
VAR Cpu = DIVIDE(USL - MeanVal, 3 * Sigma)
VAR Cpl = DIVIDE(MeanVal - LSL, 3 * Sigma)
RETURN
    MIN(Cpu, Cpl)
    
    
Worst Cp =
MINX(
    VALUES('fact_table'[Parameter]),
    CALCULATE([Cp])
)

Worst Cpk =
MINX(
    VALUES('fact_table'[Parameter]),
    CALCULATE([Cpk])
)

Worst Cp Parameter =
VAR MinCp =
    MINX(
        VALUES('fact_table'[Parameter]),
        CALCULATE([Cp])
    )
RETURN
    CALCULATE(
        SELECTEDVALUE('fact_table'[Parameter]),
        FILTER(
            VALUES('fact_table'[Parameter]),
            CALCULATE([Cp]) = MinCp
        )
    )
    
Worst Cpk Parameter =
VAR MinCpk =
    MINX(
        VALUES('fact_table'[Parameter]),
        CALCULATE([Cpk])
    )
RETURN
    CALCULATE(
        SELECTEDVALUE('fact_table'[Parameter]),
        FILTER(
            VALUES('fact_table'[Parameter']),
            CALCULATE([Cpk]) = MinCpk
        )
    )
    
Worst Overall Parameter =
IF(
    [Worst Cp Value] < [Worst Cpk Value],
    [Worst Cp Parameter],
    [Worst Cpk Parameter]
)


#######PYthon 

import pandas as pd
import numpy as np

limits = {
    "ML": (0.28, 0.40),
    "MH": (5.18, 8.92),
    "TS2": (0.98, 1.36),
    "TC90": (3.47, 4.19),
    "MOONEY": (10, 27),
    "TENSILE": (870, 1153),
    "ELONG": (600, 1432),
    "M25": (76, 112),
    "M50": (102, 156),
    "M75": (109, 199),
    "M100": (108, 252),
    "M200": (124, 496),
    "M300": (198, 666),
    "TEAR C": (123, 153),
    "DURO": (45, 55),
    "SG": (1.28, 1.32),
    "CAP": (268, 286),
    "DF": (0.15, 0.34),
    "DS": (925, 1129),
    "DC": (2.4, 2.7),
}

df = pd.read_excel("your_file.xlsx")

# Melt (same as Power BI unpivot)
df_long = df.melt(
    id_vars=["Date", "Lot", "Batch"],  # adjust if needed
    var_name="Parameter",
    value_name="Value"
)

df_long = df_long.dropna(subset=["Value"])
df_long["Value"] = df_long["Value"].astype(float)


results = []

for param, group in df_long.groupby("Parameter"):
    
    if param not in limits:
        continue
    
    LSL, USL = limits[param]
    
    values = group["Value"]
    
    mean = np.mean(values)
    sigma = np.std(values, ddof=0)  # population std (same as Power BI STDEV.P)
    
    if sigma == 0:
        cp = np.nan
        cpk = np.nan
    else:
        cp = (USL - LSL) / (6 * sigma)
        
        cpu = (USL - mean) / (3 * sigma)
        cpl = (mean - LSL) / (3 * sigma)
        cpk = min(cpu, cpl)
    
    results.append({
        "Parameter": param,
        "Mean": mean,
        "Sigma": sigma,
        "Cp": cp,
        "Cpk": cpk
    })

df_results = pd.DataFrame(results)


# Worst Cp
worst_cp_row = df_results.loc[df_results["Cp"].idxmin()]

# Worst Cpk
worst_cpk_row = df_results.loc[df_results["Cpk"].idxmin()]

print("Worst Cp Parameter:", worst_cp_row["Parameter"], "Value:", worst_cp_row["Cp"])
print("Worst Cpk Parameter:", worst_cpk_row["Parameter"], "Value:", worst_cpk_row["Cpk"])


