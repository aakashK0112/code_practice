from datetime import datetime

from src.common.db import get_connection, extract_table, load_table
from src.layers.mart.paintline_long import transform


RAW_SCHEMA = "raw"
RAW_TABLE = "paintline_raw"

MART_SCHEMA = "mart"
MART_TABLE = "paintline_long"


def truncate_table(schema: str, table: str):
    """
    Clear the existing MART table without dropping/recreating it.

    This preserves the SQL table definition, including:
      - IDENTITY ID
      - DEFAULT GETDATE() on Created_On
      - column data types
      - indexes/constraints

    The current Paint Line process is a FULL REFRESH because the
    Excel file is manually loaded into RAW using replace mode.
    """

    conn = get_connection("target")

    try:
        cur = conn.cursor()
        cur.execute(
            f"TRUNCATE TABLE [{schema}].[{table}]"
        )
        conn.commit()
    finally:
        conn.close()


def run_paintline_pipeline():
    print("=" * 70)
    print("🟦 PAINT LINE PIPELINE STARTED")
    print("=" * 70)

    run_time = datetime.now()
    print(f"Run time: {run_time}")

    # ---------------------------------------------------------
    # STEP 1: READ RAW
    # ---------------------------------------------------------
    print("\n[STEP 1] Reading RAW Paint Line data...")

    raw_df = extract_table(
        RAW_SCHEMA,
        RAW_TABLE,
        db_type="target"
    )

    if raw_df is None or raw_df.empty:
        print("⚠️ No data found in raw.paintline_raw.")
        print("Pipeline stopped.")
        return

    print(
        f"🟩 RAW rows read: {len(raw_df)}"
    )

    # ---------------------------------------------------------
    # STEP 2: TRANSFORM RAW → MART
    # ---------------------------------------------------------
    print("\n[STEP 2] Transforming Paint Line data...")

    mart_df = transform(raw_df)

    if mart_df is None or mart_df.empty:
        print("⚠️ Transform returned no data.")
        print("Pipeline stopped.")
        return

    print(
        f"🟩 MART rows prepared: {len(mart_df)}"
    )

    # ---------------------------------------------------------
    # STEP 3: FULL REFRESH MART
    # ---------------------------------------------------------
    #
    # RAW is manually replaced from Excel.
    # Therefore MART is also refreshed completely.
    #
    # We TRUNCATE instead of using pandas if_exists='replace'
    # so the existing SQL MART table structure is preserved.
    # ---------------------------------------------------------
    print(
        "\n[STEP 3] Refreshing mart.paintline_long..."
    )

    truncate_table(
        MART_SCHEMA,
        MART_TABLE
    )

    # ---------------------------------------------------------
    # STEP 4: LOAD MART
    # ---------------------------------------------------------
    print(
        "[STEP 4] Loading transformed data into MART..."
    )

    load_table(
        mart_df,
        schema=MART_SCHEMA,
        table=MART_TABLE,
        if_exists="append",
    )

    print(
        f"🟩 MART loaded: {len(mart_df)} rows"
    )

    print("\n" + "=" * 70)
    print("✅ PAINT LINE PIPELINE COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    run_paintline_pipeline()
