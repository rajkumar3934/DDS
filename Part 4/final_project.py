from constants import POSTGRES_CONFIG, DATABASE_CONFIG, TABLE_NAMES, CREATE_QUERIES
from random import random
import psycopg2
import random
from datetime import date, timedelta
from tabulate import tabulate
import threading
#from psycopg2 import extensions

def printStatements(message):
    print("======================================================================")
    print(message)
    print("======================================================================")

def connect_postgres(dbname):
    """Connect to the PostgreSQL using psycopg2 with default database
       Return the connection"""
    return psycopg2.connect(host=POSTGRES_CONFIG['HOST_NAME'], dbname=dbname, user=POSTGRES_CONFIG['USER_NAME'], password= POSTGRES_CONFIG['PASSWORD'], port=POSTGRES_CONFIG['PORT'])


def create_database(dbname):
    """Connect to the PostgreSQL by calling connect_postgres() function
       Create a database named 'dbname' passed in argument
       Close the connection"""
    
    conn = None
    cur = None
    try:
        conn = connect_postgres('postgres')
        conn.autocommit = True
        cur = conn.cursor()

        # Check if the database already exists
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
        if cur.fetchone():
            print(f"Database '{dbname}' already exists.")
        else:
            # Create the new database
            cur.execute(f"CREATE DATABASE {dbname}")
            print(f"Database '{dbname}' created successfully.")

    except Exception as error:
        print(error)

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

def execute_query(conn, query):
    """
    Executes a given SQL query using the provided database connection.

    :param conn: Database connection object
    :param query: SQL query string to be executed
    """
    try:
        # Create a new cursor
        with conn.cursor() as cur:
            # Execute the query
            cur.execute(query)

            # Print success message
            print("Query executed successfully")

            # Commit the transaction
            conn.commit()
    except (Exception, psycopg2.Error) as error:
        # Rollback in case of error
        conn.rollback()
        print("Error executing query:", error)
        raise


def create_tables(conn):
    try:
        
        execute_query(conn, CREATE_QUERIES[0])
        printStatements("Succesfully created Orders Table")
        execute_query(conn, CREATE_QUERIES[1])
        printStatements("Succesfully created Order Details Table")    
        execute_query(conn, CREATE_QUERIES[2])
        printStatements("Succesfully created Inventory Table")
        execute_query(conn, CREATE_QUERIES[3])
        printStatements("Succesfully Inserted Data in Inventory Table")
        
    except (Exception, psycopg2.Error) as error:
        print("Error creating tables:", error)



def select_data(conn):
    try:
        cursor = conn.cursor()
        if conn and cursor:
            for table_name in TABLE_NAMES:
              cursor.execute(f"SELECT * FROM {table_name};")
              rows = cursor.fetchall()
              headers = [desc[0] for desc in cursor.description]
              print(f"Data from Table: {table_name}")
              print(tabulate(rows, headers=headers, tablefmt="grid"))
              print("\n" + "-" * 50 + "\n")  # Separator between tables

    except (Exception, psycopg2.Error) as error:
        print("Error selecting data:", error)


def get_table_names(conn):
    """
    Fetches the table names from the database.

    :param conn: Database connection object
    :return: List of table names
    """
    table_names = []
    try:
        with conn.cursor() as cur:
            # Query to fetch table names, adjust schema if needed
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = cur.fetchall()
            table_names = [table[0] for table in tables]
    except (Exception, psycopg2.Error) as error:
        print("Error fetching table names:", error)
        raise
    return table_names

def delete_all_data(conn):
    for table in reversed(TABLE_NAMES):
        delete_query = f"DELETE FROM {table};"
        execute_query(conn, delete_query)

def drop_all(conn):
    """
    Drops all tables in the database.

    :param conn: Database connection object
    """
    try:
        table_names = get_table_names(conn)
        for table in reversed(table_names):
            drop_query = f"DROP TABLE IF EXISTS {table} CASCADE;"
            execute_query(conn, drop_query)
    except (Exception, psycopg2.Error) as error:
        print("Error dropping tables:", error)
        raise


def process_order(conn, user_id, product_id, quantity, order_price):
    # Each thread will use its own connection to the database
    try:
        with conn.cursor() as cursor:
            # Start a transaction
            cursor.execute("BEGIN;")

            # Insert a new order into the 'Orders' table and obtain the order ID
            cursor.execute(
                "INSERT INTO Orders (users_id, order_price, order_date) VALUES (%s, %s, CURRENT_DATE) RETURNING orders_id;",
                (user_id, order_price)
            )
            order_id = cursor.fetchone()[0]

            # Insert order details
            cursor.execute(
                "INSERT INTO OrderDetails (order_id, product_id, quantity, subtotal) VALUES (%s, %s, %s, %s);",
                (order_id, product_id, quantity, order_price)
            )

            # Update the inventory to reflect the reduced stock quantity
            cursor.execute(
                "UPDATE Inventory SET stock_quantity = stock_quantity - %s WHERE product_id = %s;",
                (quantity, product_id)
            )

            # Commit the transaction
            conn.commit()
    except psycopg2.DatabaseError as e:
        print(f"Database error occurred: {e}")
        conn.rollback()
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        if cursor is not None:
            cursor.close()

# Simulate multiple users placing orders concurrently
def simulate_concurrent_orders(conn):
    # Example data for multiple orders
    orders_data = [
        (conn, 1, 2, 1, 100.00),
        (conn, 2, 1, 2, 200.00),
        (conn, 1, 1, 1, 150.00),
        # Add more orders as needed
    ]
    
    threads = []
    for order_data in orders_data:
        # Create a new thread for each order
        thread = threading.Thread(target=process_order, args=order_data)
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()






if __name__ == '__main__':

    create_database(DATABASE_CONFIG['DATABASE_NAME'])

    with connect_postgres(dbname=DATABASE_CONFIG['DATABASE_NAME']) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        create_tables(conn)
        select_data(conn)

        # Run the simulation
        simulate_concurrent_orders(conn)
        select_data(conn)

        # delete_all_data(conn)
        # select_data(conn)
        # drop_all(conn)


        print('Done')
