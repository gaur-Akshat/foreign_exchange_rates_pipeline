import os

def get_engine():
    try:
        from dotenv import load_dotenv
    except ImportError:
        load_dotenv = None

    try:
        from sqlalchemy import create_engine
    except ImportError as exc:
        raise RuntimeError(
            "SQL loading requires sqlalchemy. Install the optional database dependencies."
        ) from exc

    if load_dotenv is not None:
        load_dotenv()

    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")

    if not all([username, password, server, database]):
        raise RuntimeError("Database environment variables are not fully configured.")

    connection_string = (
        f"mssql+pyodbc://{username}:{password}"
        f"@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    )

    return create_engine(connection_string)