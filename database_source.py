import mysql.connector

def get_source_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="eVision!@#1",
        database="samindu_db",
        port=3306
    )