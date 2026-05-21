def get_connection(db_type: str = "target"):
    config = load_config()
    db = config[f"{db_type}_db"]

    if db.get("trusted_connection", False):

        conn_str = (
            f"DRIVER={{{db['driver']}}};"
            f"SERVER={db['server']};"
            f"DATABASE={db['database']};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"
        )

    else:

        # -------------------------------
        # VALIDATION
        # -------------------------------
        username = db.get("user")
        password = db.get("password")

        if not username:
            raise ValueError(
                f"{db_type.upper()} DB username is missing in config/env"
            )

        if not password:
            raise ValueError(
                f"{db_type.upper()} DB password is missing in config/env"
            )

        conn_str = (
            f"DRIVER={{{db['driver']}}};"
            f"SERVER={db['server']};"
            f"DATABASE={db['database']};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
        )

    try:
        return pyodbc.connect(conn_str)

    except pyodbc.InterfaceError as e:

        error_msg = str(e)

        if "Login failed for user" in error_msg:
            raise Exception(
                f"{db_type.upper()} DB login failed. "
                f"Please verify username/password or DB permissions."
            )

        raise Exception(
            f"{db_type.upper()} DB connection failed: {error_msg}"
        )

    except Exception as e:

        raise Exception(
            f"{db_type.upper()} unexpected DB error: {str(e)}"
        )