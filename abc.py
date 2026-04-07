import os
import yaml
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# =========================================
# CONFIG LOADER
# =========================================
def load_config():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    config_path = os.path.join(base_dir, "config", "db_config.yaml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Inject password from env
    config["source_db"]["password"] = os.getenv("DB_PASSWORD")
    config["target_db"]["password"] = os.getenv("DB_PASSWORD")

    return config


# =========================================
# CONNECTION (PYODBC)
# =========================================
def get_connection(db_type="target"):
    config = load_config()

    db = config[f"{db_type}_db"]

    conn_str = f"""
        DRIVER={{{db['driver']}}};
        SERVER={db['server']};
        DATABASE={db['database']};
        UID={db['user']};
        PWD={db['password']};
        TrustServerCertificate=yes;
    """

    return pyodbc.connect(conn_str)


# =========================================
# SQLALCHEMY ENGINE
# =========================================
def get_engine(db_type="target"):
    config = load_config()

    db = config[f"{db_type}_db"]

    conn_str = (
        f"mssql+pyodbc://{db['user']}:{db['password']}@"
        f"{db['server']}/{db['database']}?"
        f"driver={db['driver'].replace(' ', '+')}"
    )

    return create_engine(conn_str, fast_executemany=True)


# =========================================
# GENERIC EXTRACT
# =========================================
def extract_table(schema, table, db_type="target"):
    conn = get_connection(db_type)

    query = f"SELECT * FROM {schema}.{table}"
    df = pd.read_sql(query, conn)

    conn.close()

    print(f"✅ Extracted {len(df)} rows from {schema}.{table}")
    return df


# =========================================
# GENERIC LOAD
# =========================================
def load_table(df, schema, table, db_type="target", if_exists="replace"):
    engine = get_engine(db_type)

    df.to_sql(
        table,
        engine,
        schema=schema,
        if_exists=if_exists,
        index=False
    )

    print(f"✅ Loaded to {schema}.{table}")


# =========================================
# OPTIONAL: EXECUTE QUERY
# =========================================
def execute_query(query, db_type="target"):
    conn = get_connection(db_type)
    cursor = conn.cursor()

    cursor.execute(query)
    conn.commit()

    cursor.close()
    conn.close()

    print("✅ Query executed successfully")