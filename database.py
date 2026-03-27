import mysql.connector
# define a function to get connection with the database
def get_connection():

    connection =mysql.connector.connect(
        host='localhost',
        user='root',
        password='password',
        database='order_prediction_db'
    )
    return connection
