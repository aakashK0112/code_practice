Test Parameter Count =
COUNTROWS (
    VALUES ( material_vendor_spc[parameter] )
)

Test Alert Status =
CONCATENATEX (
    VALUES ( material_vendor_spc[parameter] ),
    material_vendor_spc[parameter]
        & " = "
        & CALCULATE (
            [Window Alert Status],
            TREATAS (
                { material_vendor_spc[parameter] },
                material_long_epdm[parameter]
            )
        ),
    UNICHAR ( 10 )
)