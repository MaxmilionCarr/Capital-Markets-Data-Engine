from database.db import DataBase
import sqlite3 as sql
import os
import time
from dotenv import load_dotenv

load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")

def test_database_create(path = test_env_path):
    print("Creating database...")
    try:
        db = DataBase(path)
        db.create_db()
    except Exception as e:
        print("Failed to create database:", e)
        return -1
    return 1

def test_database_delete(path = test_env_path):
    print("Deleting database...")
    try:
        db = DataBase(path)
        db.delete_db()
        time.sleep(2)
    except Exception as e:
        print("Failed to delete database:", e)
        return -1
    return 1

def basic_tests():
    print("CORE DATABASE TESTS")
    run = 0
    run += test_database_delete()
    time.sleep(2)
    run += test_database_create()

    if run != 2:
        print("Database tests failed.")
        return -1
    else:
        print("Database tests passed.")
        return 1
    
def database_tests():
    print("DATABASE TEST SUITE")
    basic_tests()
    # Add more database related tests here in the future