import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def create_database_if_not_exists():
    load_dotenv()
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD", "")
    server = os.getenv("DB_SERVER", "localhost")
    database = os.getenv("DB_NAME")

    sys_conn = f"mysql+mysqlconnector://{username}:{password}@{server}/"
    engine = create_engine(sys_conn)

    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {database}"))

    engine.dispose()

def get_engine():
    load_dotenv()
    
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD", "")
    server = os.getenv("DB_SERVER", "localhost")
    database = os.getenv("DB_NAME")
    connection_string = f"mysql+mysqlconnector://{username}:{password}@{server}/{database}"
    
    return create_engine(connection_string)