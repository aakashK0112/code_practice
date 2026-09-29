Bucket Alert Count =
VAR _SelectedStatus =
    SELECTEDVALUE ( Alert_Status[Alert Status] )

RETURN
SUMX (
    VALUES ( material_vendor_spc[parameter] ),

    VAR _Parameter =
        material_vendor_spc[parameter]

    VAR _Status =
        CALCULATE (
            [Window Alert Status],
            TREATAS (
                { _Parameter },
                material_long_epdm[parameter]
            )
        )

    VAR _StatusCategory =
        SWITCH (
            TRUE(),

            CONTAINSSTRING ( _Status, "Critical" ),
                "Critical",

            CONTAINSSTRING ( _Status, "Warning" ),
                "Warning",

            CONTAINSSTRING ( _Status, "Attention" ),
                "Attention",

            CONTAINSSTRING ( _Status, "In Control" ),
                "In Control",

            BLANK()
        )

    RETURN
        IF (
            _StatusCategory = _SelectedStatus,
            1,
            0
        )
)


Alert Category =
VAR _Status =
    [Window Alert Status]

RETURN
SWITCH (
    TRUE(),

    CONTAINSSTRING ( _Status, "Critical" ),
        "Critical",

    CONTAINSSTRING ( _Status, "Warning" ),
        "Warning",

    CONTAINSSTRING ( _Status, "Attention" ),
        "Attention",

    CONTAINSSTRING ( _Status, "In Control" ),
        "In Control",

    BLANK()
)