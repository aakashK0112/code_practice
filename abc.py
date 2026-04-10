DimDate_Material =
VAR MinDate = MIN(material_long[Date])
VAR MaxDate = MAX(material_long[Date])
RETURN
ADDCOLUMNS(
    CALENDAR(MinDate, MaxDate),

    // =========================
    // BASE DATE
    // =========================
    "Material_Date", [Date],

    // =========================
    // DISPLAY COLUMNS
    // =========================
    "Material_Date_Display", FORMAT([Date], "dd-MMM-yyyy"),

    "Material_Week",
        YEAR([Date]) & "-W" & FORMAT(WEEKNUM([Date], 2), "00"),

    "Material_Month",
        FORMAT([Date], "MMM yyyy"),

    "Material_Quarter",
        "Q" & FORMAT([Date], "Q") & " " & YEAR([Date]),

    // =========================
    // SORT COLUMNS (VERY IMPORTANT)
    // =========================
    "Material_Date_Sort", [Date],

    "Material_Week_Sort",
        YEAR([Date]) * 100 + WEEKNUM([Date], 2),

    "Material_Month_Sort",
        YEAR([Date]) * 100 + MONTH([Date]),

    "Material_Quarter_Sort",
        YEAR([Date]) * 10 + VALUE(FORMAT([Date], "Q"))
)




material_long[Date]  →  DimDate_Material[Material_Date]


Do this one by one:
1. Date
Column: Material_Date_Display
Sort by → Material_Date_Sort
2. Week
Column: Material_Week
Sort by → Material_Week_Sort
3. Month
Column: Material_Month
Sort by → Material_Month_Sort
4. Quarter
Column: Material_Quarter
Sort by → Material_Quarter_Sort


Material Time Axis =
{
    ("Day", NAMEOF('DimDate_Material'[Material_Date_Display]), 0),
    ("Week", NAMEOF('DimDate_Material'[Material_Week]), 1),
    ("Month", NAMEOF('DimDate_Material'[Material_Month]), 2),
    ("Quarter", NAMEOF('DimDate_Material'[Material_Quarter]), 3)
}

