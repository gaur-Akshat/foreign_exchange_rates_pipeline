import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")

    connection_string = (
        f"mssql+pyodbc://{username}:{password}"
        f"@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    )

    return create_engine(connection_string)