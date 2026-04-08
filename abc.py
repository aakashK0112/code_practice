def get_connection(db_type="target"):
    config = load_config()
    db = config[f"{db_type}_db"]

    if db.get("trusted_connection", False):
        conn_str = f"""
            DRIVER={{{db['driver']}}};
            SERVER={db['server']};
            DATABASE={db['database']};
            Trusted_Connection=yes;
            TrustServerCertificate=yes;
        """
    else:
        conn_str = f"""
            DRIVER={{{db['driver']}}};
            SERVER={db['server']};
            DATABASE={db['database']};
            UID={db['user']};
            PWD={db['password']};
            TrustServerCertificate=yes;
        """

    return pyodbc.connect(conn_str)

def get_engine(db_type="target"):
    config = load_config()
    db = config[f"{db_type}_db"]

    if db.get("trusted_connection", False):
        conn_str = (
            f"mssql+pyodbc://@{db['server']}/{db['database']}?"
            f"driver={db['driver'].replace(' ', '+')}"
            "&trusted_connection=yes"
        )
    else:
        conn_str = (
            f"mssql+pyodbc://{db['user']}:{db['password']}@"
            f"{db['server']}/{db['database']}?"
            f"driver={db['driver'].replace(' ', '+')}"
        )

    return create_engine(conn_str, fast_executemany=True)

