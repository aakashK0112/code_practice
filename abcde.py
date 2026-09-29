Cp =
VAR CurrentMaterial =
    SELECTEDVALUE(material_vendor_spc[material_id])

VAR CurrentParameter =
    SELECTEDVALUE(material_vendor_spc[parameter])

VAR USL =
    CALCULATE(
        MAX(material_limits_epdm[upper_limit]),
        FILTER(
            ALL(material_limits_epdm),
            material_limits_epdm[material_id] = CurrentMaterial
                && material_limits_epdm[parameter] = CurrentParameter
        )
    )

VAR LSL =
    CALCULATE(
        MAX(material_limits_epdm[lower_limit]),
        FILTER(
            ALL(material_limits_epdm),
            material_limits_epdm[material_id] = CurrentMaterial
                && material_limits_epdm[parameter] = CurrentParameter
        )
    )

VAR Sigma =
    STDEV.S(material_vendor_spc[value])

RETURN
IF(
    ISBLANK(USL)
        || ISBLANK(LSL)
        || ISBLANK(Sigma)
        || Sigma = 0,
    BLANK(),
    DIVIDE(
        USL - LSL,
        6 * Sigma
    )
)


Cpk =
VAR CurrentMaterial =
    SELECTEDVALUE(material_vendor_spc[material_id])

VAR CurrentParameter =
    SELECTEDVALUE(material_vendor_spc[parameter])

VAR USL =
    CALCULATE(
        MAX(material_limits_epdm[upper_limit]),
        FILTER(
            ALL(material_limits_epdm),
            material_limits_epdm[material_id] = CurrentMaterial
                && material_limits_epdm[parameter] = CurrentParameter
        )
    )

VAR LSL =
    CALCULATE(
        MAX(material_limits_epdm[lower_limit]),
        FILTER(
            ALL(material_limits_epdm),
            material_limits_epdm[material_id] = CurrentMaterial
                && material_limits_epdm[parameter] = CurrentParameter
        )
    )

VAR MeanValue =
    AVERAGE(material_vendor_spc[value])

VAR Sigma =
    STDEV.S(material_vendor_spc[value])

VAR Cpu =
    DIVIDE(
        USL - MeanValue,
        3 * Sigma
    )

VAR Cpl =
    DIVIDE(
        MeanValue - LSL,
        3 * Sigma
    )

RETURN
IF(
    ISBLANK(USL)
        || ISBLANK(LSL)
        || ISBLANK(Sigma)
        || Sigma = 0,
    BLANK(),
    MIN(Cpu, Cpl)
)


Worst Cp Value =
MINX(
    VALUES(material_vendor_spc[parameter]),
    CALCULATE([Cp])
)

Worst Cp Parameter =
VAR ParameterTable =
    ADDCOLUMNS(
        VALUES(material_vendor_spc[parameter]),
        "__Cp",
            CALCULATE([Cp])
    )

VAR WorstParameter =
    TOPN(
        1,
        ParameterTable,
        [__Cp],
        ASC,
        material_vendor_spc[parameter],
        ASC
    )

RETURN
    MAXX(
        WorstParameter,
        material_vendor_spc[parameter]
    )
    
    
Worst Cpk Value =
MINX(
    VALUES(material_vendor_spc[parameter]),
    CALCULATE([Cpk])
)


Worst Cpk Parameter =
VAR ParameterTable =
    ADDCOLUMNS(
        VALUES(material_vendor_spc[parameter]),
        "__Cpk",
            CALCULATE([Cpk])
    )

VAR WorstParameter =
    TOPN(
        1,
        ParameterTable,
        [__Cpk],
        ASC,
        material_vendor_spc[parameter],
        ASC
    )

RETURN
    MAXX(
        WorstParameter,
        material_vendor_spc[parameter]
    )
    
    
                    Material A53MX
                          │
                          ▼
                material_vendor_spc
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
          Rheology      Physical    Electrical
              │           │           │
          5 parameters  10 parameters 6 parameters
              │           │           │
              ▼           ▼           ▼
          Calculate Cp/Cpk for each parameter
              │           │           │
              ▼           ▼           ▼
           MIN Cp       MIN Cp       MIN Cp
              │           │           │
              ▼           ▼           ▼
          Parameter     Parameter    Parameter
          
          
Worst Cp Value =
VAR ParameterTable =
    ADDCOLUMNS(
        VALUES(material_vendor_spc[parameter]),
        "__Cp",
            CALCULATE([Cp Vendor])
    )
RETURN
    MINX(
        FILTER(
            ParameterTable,
            NOT ISBLANK([__Cp])
        ),
        [__Cp]
    )
    
    
Worst Cp Parameter =
VAR ParameterTable =
    ADDCOLUMNS(
        VALUES(material_vendor_spc[parameter]),
        "__Cp",
            CALCULATE([Cp Vendor])
    )

VAR WorstRow =
    TOPN(
        1,
        FILTER(
            ParameterTable,
            NOT ISBLANK([__Cp])
        ),
        [__Cp], ASC,
        material_vendor_spc[parameter], ASC
    )

RETURN
    MAXX(
        WorstRow,
        material_vendor_spc[parameter]
    )
    
Worst Cpk Value =
VAR ParameterTable =
    ADDCOLUMNS(
        VALUES(material_vendor_spc[parameter]),
        "__Cpk",
            CALCULATE([Cpk Vendor])
    )
RETURN
    MINX(
        FILTER(
            ParameterTable,
            NOT ISBLANK([__Cpk])
        ),
        [__Cpk]
    )
    
Worst Cpk Parameter =
VAR ParameterTable =
    ADDCOLUMNS(
        VALUES(material_vendor_spc[parameter]),
        "__Cpk",
            CALCULATE([Cpk Vendor])
    )

VAR WorstRow =
    TOPN(
        1,
        FILTER(
            ParameterTable,
            NOT ISBLANK([__Cpk])
        ),
        [__Cpk], ASC,
        material_vendor_spc[parameter], ASC
    )

RETURN
    MAXX(
        WorstRow,
        material_vendor_spc[parameter]
    )
    
    
    
    
Lower Limit =
VAR _Parameter =
    SELECTEDVALUE(material_long_epdm[parameter])

RETURN
CALCULATE(
    MIN(material_limits_epdm[lower_limit]),
    TREATAS(
        {_Parameter},
        material_limits_epdm[parameter]
    )
)


Upper Limit =
VAR _Parameter =
    SELECTEDVALUE(material_long_epdm[parameter])

RETURN
CALCULATE(
    MAX(material_limits_epdm[upper_limit]),
    TREATAS(
        {_Parameter},
        material_limits_epdm[parameter]
    )
)


Lower Limit =
VAR _MaterialID =
    SELECTEDVALUE(material_long_epdm[material_id])

VAR _Parameter =
    SELECTEDVALUE(material_long_epdm[parameter])

RETURN
CALCULATE(
    MIN(material_limits_epdm[lower_limit]),
    TREATAS(
        {_MaterialID},
        material_limits_epdm[material_id]
    ),
    TREATAS(
        {_Parameter},
        material_limits_epdm[parameter]
    )
)

Upper Limit =
VAR _MaterialID =
    SELECTEDVALUE(material_long_epdm[material_id])

VAR _Parameter =
    SELECTEDVALUE(material_long_epdm[parameter])

RETURN
CALCULATE(
    MAX(material_limits_epdm[upper_limit]),
    TREATAS(
        {_MaterialID},
        material_limits_epdm[material_id]
    ),
    TREATAS(
        {_Parameter},
        material_limits_epdm[parameter]
    )
)


Window Risk Level =
VAR _CpkVal =
    [Cpk Vendor]

RETURN
SWITCH (
    TRUE (),

    ISBLANK ( _CpkVal ),
        BLANK (),

    _CpkVal < 1.00,
        "HIGH",

    _CpkVal < 1.33,
        "MEDIUM",

    "LOW"
)


Window Has OOS =
VAR _LSL =
    [LSL Vendor]

VAR _USL =
    [USL Vendor]

RETURN
IF (
    ISBLANK ( _LSL )
        || ISBLANK ( _USL ),
    BLANK (),

    CALCULATE (
        COUNTROWS ( material_vendor_spc ),
        FILTER (
            material_vendor_spc,
            material_vendor_spc[value] < _LSL
                || material_vendor_spc[value] > _USL
        )
    )
)


Window Trend Direction =
VAR _Curr =
    [Material Average Value]

VAR _Prev =
    CALCULATE (
        [Material Average Value],
        DATEADD (
            DimDate_MaterialVendor[Material_Date],
            -1,
            DAY
        )
    )

RETURN
IF (
    ISBLANK ( _Curr )
        || ISBLANK ( _Prev ),
    BLANK (),
    IF (
        _Curr > _Prev,
        "UP",
        "DOWN"
    )
)


Window Alert Status =
VAR _HasOOS =
    [Window Has OOS]

VAR _Trend =
    UPPER ( [Window Trend Direction] )

VAR _Risk =
    UPPER ( [Window Risk Level] )

VAR _Parameter =
    SELECTEDVALUE (
        material_vendor_spc[parameter],
        "Multiple Parameters"
    )

RETURN
SWITCH (
    TRUE (),

    -- Priority 1: Actual value outside specification
    _HasOOS > 0,
        "🔴 Critical - Out Of Spec (" & _Parameter & ")",

    -- Priority 2: High capability risk and trending upward
    _Risk = "HIGH"
        && _Trend = "UP",
        "🟠 Warning - Trending To Limit (" & _Parameter & ")",

    -- Priority 3: Medium capability risk
    _Risk = "MEDIUM",
        "🟡 Attention - Capability Risk (" & _Parameter & ")",

    -- Normal condition
    "🟢 In Control (" & _Parameter & ")"
)


Window Alert Color =
VAR _HasOOS =
    [Window Has OOS]

VAR _Trend =
    UPPER ( [Window Trend Direction] )

VAR _Risk =
    UPPER ( [Window Risk Level] )

RETURN
SWITCH (
    TRUE (),

    _HasOOS > 0,
        "#D32F2F",

    _Risk = "HIGH"
        && _Trend = "UP",
        "#F57C00",

    _Risk = "MEDIUM",
        "#FBC02D",

    "#2E7D32"
)