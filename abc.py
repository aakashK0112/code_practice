USL_PD = 5


Parts_Tested_N_PD120 =
CALCULATE(
    DISTINCTCOUNT(pdtester_pbci[PID_Tested]),
    NOT(ISBLANK(pdtester_pbci[PD_120]))
)


P95_PD120 =
PERCENTILEX.INC(
    FILTER(
        pdtester_pbci,
        NOT(ISBLANK(pdtester_pbci[PD_120]))
    ),
    pdtester_pbci[PD_120],
    0.95
)


Median_PD120 =
VAR P95_Value =
    [P95_PD120]
RETURN
MEDIANX(
    FILTER(
        pdtester_pbci,
        NOT(ISBLANK(pdtester_pbci[PD_120])) &&
        pdtester_pbci[PD_120] <= P95_Value
    ),
    pdtester_pbci[PD_120]
)

PDCI_PD120 =
VAR MedianVal = [Median_PD120]
VAR P95Val = [P95_PD120]
VAR USLVal = [USL_PD]
VAR Denominator = P95Val - MedianVal
RETURN
IF(
    Denominator = 0,
    BLANK(),
    DIVIDE(USLVal - MedianVal, Denominator)
)
