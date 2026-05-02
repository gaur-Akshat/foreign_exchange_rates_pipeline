import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def _env():
    load_dotenv()
    return (
        os.getenv("DB_USERNAME"),
        os.getenv("DB_PASSWORD", ""),
        os.getenv("DB_SERVER", "localhost"),
        os.getenv("DB_NAME"),
    )

def create_database_if_not_exists():
    user, pw, server, db = _env()
    engine = create_engine(f"mysql+mysqlconnector://{user}:{pw}@{server}/")
    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db}"))
    engine.dispose()

def get_engine():
    user, pw, server, db = _env()
    return create_engine(f"mysql+mysqlconnector://{user}:{pw}@{server}/{db}")