DimDate_Shift =
VAR MinDate =
    MINX(
        UNION(
            SELECTCOLUMNS(pdtester_tested_parts_dist, "D", pdtester_tested_parts_dist[ShiftDate]),
            SELECTCOLUMNS(pdtester_tpy, "D", pdtester_tpy[ShiftDate]),
            SELECTCOLUMNS(pdtester_fpy_shift, "D", pdtester_fpy_shift[ShiftDate]),
            SELECTCOLUMNS(pyrometer_long, "D", pyrometer_long[ShiftDate])
        ),
        [D]
    )

VAR MaxDate =
    MAXX(
        UNION(
            SELECTCOLUMNS(pdtester_tested_parts_dist, "D", pdtester_tested_parts_dist[ShiftDate]),
            SELECTCOLUMNS(pdtester_tpy, "D", pdtester_tpy[ShiftDate]),
            SELECTCOLUMNS(pdtester_fpy_shift, "D", pdtester_fpy_shift[ShiftDate]),
            SELECTCOLUMNS(pyrometer_long, "D", pyrometer_long[ShiftDate])
        ),
        [D]
    )

RETURN
ADDCOLUMNS(
    CALENDAR(MinDate, MaxDate),

    // =========================
    // BASE
    // =========================
    "Shift_Date", [Date],

    // =========================
    // DISPLAY COLUMNS (STANDARDIZED)
    // =========================
    "Shift_Date_Display",
        FORMAT([Date], "dd-MMM-yyyy"),

    "Shift_Week",
        YEAR([Date]) & "-W" & FORMAT(WEEKNUM([Date], 2), "00"),

    "Shift_Month",
        FORMAT([Date], "MMM yyyy"),

    "Shift_Quarter",
        "Q" & FORMAT([Date], "Q") & " " & YEAR([Date]),

    // =========================
    // SORT COLUMNS (CRITICAL)
    // =========================
    "Shift_Date_Sort",
        [Date],

    "Shift_Week_Sort",
        YEAR([Date]) * 100 + WEEKNUM([Date], 2),

    "Shift_Month_Sort",
        YEAR([Date]) * 100 + MONTH([Date]),

    "Shift_Quarter_Sort",
        YEAR([Date]) * 10 + VALUE(FORMAT([Date], "Q"))
)



Shift Time Axis =
{
    ("Day", NAMEOF('DimDate_Shift'[Shift_Date_Display]), 0),
    ("Week", NAMEOF('DimDate_Shift'[Shift_Week]), 1),
    ("Month", NAMEOF('DimDate_Shift'[Shift_Month]), 2),
    ("Quarter", NAMEOF('DimDate_Shift'[Shift_Quarter]), 3)
}






DimDate_Mould =
VAR MinDate =
    MIN ( prod_vs_qr_vs_tested_parts[Mould_Date] )

VAR MaxDate =
    MAX ( prod_vs_qr_vs_tested_parts[Mould_Date] )

RETURN
ADDCOLUMNS(
    CALENDAR(MinDate, MaxDate),

    // =========================
    // BASE DATE
    // =========================
    "Mould_Date", [Date],

    // =========================
    // DISPLAY COLUMNS
    // =========================
    "Mould_Date_Display",
        FORMAT([Date], "dd-MMM-yyyy"),

    "Mould_Week",
        YEAR([Date]) & "-W" & FORMAT(WEEKNUM([Date], 2), "00"),

    "Mould_Month",
        FORMAT([Date], "MMM yyyy"),

    "Mould_Quarter",
        "Q" & FORMAT([Date], "Q") & " " & YEAR([Date]),

    // =========================
    // SORT COLUMNS (CRITICAL)
    // =========================
    "Mould_Date_Sort",
        [Date],

    "Mould_Week_Sort",
        YEAR([Date]) * 100 + WEEKNUM([Date], 2),

    "Mould_Month_Sort",
        YEAR([Date]) * 100 + MONTH([Date]),

    "Mould_Quarter_Sort",
        YEAR([Date]) * 10 + VALUE(FORMAT([Date], "Q"))
)



Mould Time Axis =
{
    ("Day", NAMEOF('DimDate_Mould'[Mould_Date_Display]), 0),
    ("Week", NAMEOF('DimDate_Mould'[Mould_Week]), 1),
    ("Month", NAMEOF('DimDate_Mould'[Mould_Month]), 2),
    ("Quarter", NAMEOF('DimDate_Mould'[Mould_Quarter]), 3)
}


