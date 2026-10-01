import pandas as pd


PRODUCT = "BI_25KV_BUSHING_INSERT"


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw Paint Line wide data into the Power BI-ready
    mart.paintline_long structure.

    Expected RAW source columns:
        Fecha / Paint_Test_Date
        Turno / Shift
        Núm. de Parte / Núm._de_Parte / Part_Number
        Zona 1 / Zona_1 / Z1
        Zona 2 / Zona_2 / Z2
        Zona 3 / Zona_3 / Z3
        Zona 4 / Zona_4 / Z4
    """

    print("🟦 Paint Line MART transform started...")

    if df is None or df.empty:
        print("⚠️ No RAW Paint Line data found.")
        return pd.DataFrame(
            columns=[
                "Product",
                "Part_Number",
                "Paint_Test_Date",
                "Shift",
                "Zone",
                "Paint_Thickness",
            ]
        )

    df = df.copy()

    # ---------------------------------------------------------
    # 1. Remove unwanted Excel columns
    # ---------------------------------------------------------
    df = df.loc[
        :,
        [c for c in df.columns if not str(c).startswith("Unnamed:")]
    ]

    # ---------------------------------------------------------
    # 2. Normalize possible RAW column names
    # ---------------------------------------------------------
    rename_map = {
        "Fecha": "Paint_Test_Date",
        "fecha": "Paint_Test_Date",

        "Turno": "Shift",
        "turno": "Shift",

        "Núm. de Parte": "Part_Number",
        "Núm._de_Parte": "Part_Number",
        "Num. de Parte": "Part_Number",
        "Num._de_Parte": "Part_Number",

        "Zona 1": "Z1",
        "Zona_1": "Z1",
        "ZONA 1": "Z1",

        "Zona 2": "Z2",
        "Zona_2": "Z2",
        "ZONA 2": "Z2",

        "Zona 3": "Z3",
        "Zona_3": "Z3",
        "ZONA 3": "Z3",

        "Zona 4": "Z4",
        "Zona_4": "Z4",
        "ZONA 4": "Z4",
    }

    df = df.rename(
        columns={
            old: new
            for old, new in rename_map.items()
            if old in df.columns
        }
    )

    required_columns = [
        "Paint_Test_Date",
        "Shift",
        "Part_Number",
        "Z1",
        "Z2",
        "Z3",
        "Z4",
    ]

    missing_columns = [
        col for col in required_columns
        if col not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Paint Line RAW is missing required columns: {missing_columns}"
        )

    # ---------------------------------------------------------
    # 3. Keep only required columns
    # ---------------------------------------------------------
    df = df[required_columns].copy()

    # Keep original RAW row order.
    # This is important because the same Product/Part/Date/Shift
    # can have more than one Paint Line record.
    df["_Source_Row_No"] = range(1, len(df) + 1)

    # ---------------------------------------------------------
    # 4. Data type cleaning
    # ---------------------------------------------------------
    df["Paint_Test_Date"] = pd.to_datetime(
        df["Paint_Test_Date"],
        errors="coerce"
    )

    df["Shift"] = pd.to_numeric(
        df["Shift"],
        errors="coerce"
    ).astype("Int64")

    df["Part_Number"] = (
        df["Part_Number"]
        .astype("string")
        .str.strip()
    )

    for col in ["Z1", "Z2", "Z3", "Z4"]:
        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        )

    # Remove invalid header/data rows
    df = df.dropna(
        subset=["Paint_Test_Date", "Part_Number"]
    ).copy()

    # ---------------------------------------------------------
    # 5. Add Product
    # ---------------------------------------------------------
    df["Product"] = PRODUCT

    # ---------------------------------------------------------
    # 6. Convert wide → long
    # ---------------------------------------------------------
    df_long = pd.melt(
        df,
        id_vars=[
            "Product",
            "Part_Number",
            "Paint_Test_Date",
            "Shift",
            "_Source_Row_No",
        ],
        value_vars=["Z1", "Z2", "Z3", "Z4"],
        var_name="Zone",
        value_name="Paint_Thickness",
    )

    # Remove zones where no measurement exists
    df_long = df_long.dropna(
        subset=["Paint_Thickness"]
    ).copy()

    # ---------------------------------------------------------
    # 7. Sort while preserving duplicate test records
    # ---------------------------------------------------------
    zone_order = {"Z1": 1, "Z2": 2, "Z3": 3, "Z4": 4}
    df_long["_Zone_Order"] = df_long["Zone"].map(zone_order)

    df_long = df_long.sort_values(
        by=[
            "Paint_Test_Date",
            "Part_Number",
            "Shift",
            "_Source_Row_No",
            "_Zone_Order",
        ],
        kind="stable",
    ).reset_index(drop=True)

    # ---------------------------------------------------------
    # 8. Final MART columns
    #
    # ID is intentionally NOT generated here.
    # mart.paintline_long should generate ID from its SQL
    # IDENTITY column.
    #
    # Created_On is also intentionally omitted so SQL Server
    # can populate it through DEFAULT GETDATE().
    # ---------------------------------------------------------
    df_long = df_long[
        [
            "Product",
            "Part_Number",
            "Paint_Test_Date",
            "Shift",
            "Zone",
            "Paint_Thickness",
        ]
    ]

    print(
        f"🟩 Paint Line MART transform completed: "
        f"{len(df_long)} rows"
    )

    return df_long
