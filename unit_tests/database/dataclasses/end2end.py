from database.db import DataBase
import os
from dotenv import load_dotenv

load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")

