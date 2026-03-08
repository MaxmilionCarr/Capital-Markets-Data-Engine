import dotenv
import os
from database_connector.db import DataBase
dotenv.load_dotenv()

db_path = os.getenv("TESTING_DATABASE_PATH")

def create_test_database():
    database = DataBase(db_path)
    database.create_db()

if __name__ == "__main__":
    create_test_database()