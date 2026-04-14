material_limits[parameter] → FactTable[Parameter]

W1 Base Value =
AVERAGE('material_long'[Value])

W1 Value =
SWITCH(
    TRUE(),

    -- Batch level
    ISINSCOPE('material_long'[Batch]),
        AVERAGE('material_long'[Value]),

    -- Lot level
    ISINSCOPE('material_long'[Lot]),
        CALCULATE(
            AVERAGE('material_long'[Value]),
            ALLEXCEPT(
                'material_long',
                'material_long'[Parameter],
                'material_long'[Lot]
            )
        ),

    -- Parameter level
    ISINSCOPE('material_long'[Parameter]),
        CALCULATE(
            AVERAGE('material_long'[Value]),
            ALLEXCEPT(
                'material_long',
                'material_long'[Parameter]
            )
        )
)


W1 Value Final =
CALCULATE(
    [W1 Value],
    KEEPFILTERS(
        VALUES('dim_materialdate'[Material_TestDate])
    )
)


W1 UCL =
CALCULATE(
    MAX('material_limits'[upper_limit]),
    REMOVEFILTERS('dim_materialdate')
)


W1 LCL =
CALCULATE(
    MIN('material_limits'[lower_limit]),
    REMOVEFILTERS('dim_materialdate')
)


W1 Mean =
CALCULATE(
    AVERAGE('material_long'[Value]),
    ALLSELECTED('dim_materialdate'[Material_TestDate])
)


W1 Alert =
VAR val = [W1 Value Final]
VAR ucl = [W1 UCL]
VAR lcl = [W1 LCL]
RETURN
SWITCH(
    TRUE(),
    val > ucl, "Above UCL",
    val < lcl, "Below LCL",
    "In Control"
)

W1 Value Group1 =
CALCULATE(
    [W1 Value Final],
    'material_parameter_group'[Parameter_Group] = "Group1"
)

