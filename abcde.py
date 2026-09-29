Cp =
VAR USL =
    MAX(material_limits_epdm[upper_limit])

VAR LSL =
    MAX(material_limits_epdm[lower_limit])

VAR Sigma =
    STDEV.S(material_vendor_spc[value])

RETURN
IF(
    NOT ISBLANK(Sigma)
        && Sigma > 0
        && NOT ISBLANK(USL)
        && NOT ISBLANK(LSL),
    DIVIDE(
        USL - LSL,
        6 * Sigma
    )
)


Cpk =
VAR USL =
    MAX(material_limits_epdm[upper_limit])

VAR LSL =
    MAX(material_limits_epdm[lower_limit])

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
    NOT ISBLANK(Sigma)
        && Sigma > 0
        && NOT ISBLANK(USL)
        && NOT ISBLANK(LSL),
    MIN(Cpu, Cpl)
)


