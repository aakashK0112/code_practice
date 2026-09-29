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