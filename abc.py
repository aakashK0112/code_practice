Parts_Tested =
SWITCH(
    SELECTEDVALUE(Metric_Table[Metric]),
    "PD120%", [Parts_Tested_N_PD120],
    "PD100%", [Parts_Tested_N_PD100],
    "PD Locate", [Parts_Tested_N_PDLocate]
)


Median_Selected =
SWITCH(
    SELECTEDVALUE(Metric_Table[Metric]),
    "PD120%", [Median_PD120],
    "PD100%", [Median_PD100],
    "PD Locate", [Median_PDLocate]
)

P95_Selected =
SWITCH(
    SELECTEDVALUE(Metric_Table[Metric]),
    "PD120%", [P95_PD120],
    "PD100%", [P95_PD100],
    "PD Locate", [P95_PDLocate]
)


PDCI_Selected =
SWITCH(
    SELECTEDVALUE(Metric_Table[Metric]),
    "PD120%", [PDCI_PD120],
    "PD100%", [PDCI_PD100],
    "PD Locate", [PDCI_PDLocate]
)


Sort_Order =
SWITCH(
    Metric_Table[Metric],
    "PD120%", 1,
    "PD100%", 2,
    "PD Locate", 3
)