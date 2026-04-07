Parts_Tested_N_PD120 =
CALCULATE(
    DISTINCTCOUNT(pdtester_pbci[PID_Tested]),
    NOT(ISBLANK(pdtester_pbci[PD_120]))
)

Parts_Tested_N_PD100 =
CALCULATE(
    DISTINCTCOUNT(pdtester_pbci[PID_Tested]),
    NOT(ISBLANK(pdtester_pbci[PD_100]))
)

Parts_Tested_N_PDLocate =
CALCULATE(
    DISTINCTCOUNT(pdtester_pbci[PID_Tested]),
    NOT(ISBLANK(pdtester_pbci[PD_Locate]))
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

P95_PD100 =
PERCENTILEX.INC(
    FILTER(
        pdtester_pbci,
        NOT(ISBLANK(pdtester_pbci[PD_100]))
    ),
    pdtester_pbci[PD_100],
    0.95
)

P95_PDLocate =
PERCENTILEX.INC(
    FILTER(
        pdtester_pbci,
        NOT(ISBLANK(pdtester_pbci[PD_Locate]))
    ),
    pdtester_pbci[PD_Locate],
    0.95
)

Median_pC95_PD120 =
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


Median_pC95_PD100 =
VAR P95_Value =
    [P95_PD100]
RETURN
MEDIANX(
    FILTER(
        pdtester_pbci,
        NOT(ISBLANK(pdtester_pbci[PD_100])) &&
        pdtester_pbci[PD_100] <= P95_Value
    ),
    pdtester_pbci[PD_100]
)

Median_pC95_PDLocate =
VAR P95_Value =
    [P95_PDLocate]
RETURN
MEDIANX(
    FILTER(
        pdtester_pbci,
        NOT(ISBLANK(pdtester_pbci[PD_Locate])) &&
        pdtester_pbci[PD_Locate] <= P95_Value
    ),
    pdtester_pbci[PD_Locate]
)

PDCI_PD120 =
DIVIDE(
    [Median_pC95_PD120],
    [P95_PD120]
)

PDCI_PD100 =
DIVIDE(
    [Median_pC95_PD100],
    [P95_PD100]
)


PDCI_PDLocate =
DIVIDE(
    [Median_pC95_PDLocate],
    [P95_PDLocate]
)