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

