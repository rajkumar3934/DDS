from constants import POSTGRES_CONFIG, DATABASE_CONFIG, TABLE_NAMES, CREATE_QUERIES
from random import random
import psycopg2
import random
from datetime import date, timedelta
from tabulate import tabulate
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
        printStatements("Succesfully created User Table")
        execute_query(conn, CREATE_QUERIES[1])
        printStatements("Succesfully created Categories Table")
        execute_query(conn, CREATE_QUERIES[2])
        printStatements("Succesfully created Products Table")
        execute_query(conn, CREATE_QUERIES[3])
        printStatements("Succesfully created Orders Table")
        execute_query(conn, CREATE_QUERIES[4])
        printStatements("Succesfully created Order Details Table")
        execute_query(conn, CREATE_QUERIES[5])
        printStatements("Succesfully created Transactions Table")
        execute_query(conn, CREATE_QUERIES[6])
        printStatements("Succesfully created Reviews Table")
        execute_query(conn, CREATE_QUERIES[7])
        printStatements("Succesfully created Inventory Table")
        execute_query(conn, CREATE_QUERIES[8])
        printStatements("Succesfully created Shipping Table")
        create_partition_table_users(conn)
    except (Exception, psycopg2.Error) as error:
        print("Error creating tables:", error)


# Fragment on Users - Range partitioning
def create_partition_table_users(conn):
    try:
        cursor = conn.cursor()
        # Creating partition tables
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS Users_Old PARTITION OF Users
                            FOR VALUES FROM ('2010-01-01 00:00:00') TO ('2015-12-31 00:00:00');""")

        cursor.execute(f"""CREATE TABLE IF NOT EXISTS Users_New PARTITION OF Users
                            FOR VALUES FROM ('2016-01-01 00:00:00') TO ('2023-12-31 00:00:00');""")
        conn.commit()

        print("Tables with horizontal partitions created successfully!")

    except (Exception, psycopg2.Error) as error:
        print("Error creating partitions:", error)


def insert_mock_data(conn):
    try:
        cursor = conn.cursor()
        if conn and cursor:
            insert_statement = f"""
                INSERT INTO {TABLE_NAMES[0]} (username, first_name, last_name, email, password, address, phone_number) 
VALUES 
    ('user1', 'first1', 'last1', 'user1@example.com', 'password1', '123 Main St, City1', '123-456-7890'),
    ('user2', 'first2', 'last2', 'user2@example.com', 'password2', '456 Elm St, City2', '234-567-8901'),
    ('user3', 'first3', 'last3', 'user3@example.com', 'password3', '789 Oak St, City3', '345-678-9012');
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {TABLE_NAMES[1]} (category_name) 
VALUES 
    ('Electronics'),
    ('Clothing'),
    ('Home & Kitchen');
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {TABLE_NAMES[2]} (product_name, description, product_price, category_id, product_quantity) 
VALUES 
    ('Smartphone', 'Latest smartphone model', 499.99, 1, 50),
    ('Laptop', 'High-performance laptop', 899.99, 1, 30),
    ('T-shirt', 'Cotton T-shirt', 19.99, 2, 100),
    ('Dress Shirt', 'Formal dress shirt', 29.99, 2, 80),
    ('Cookware Set', 'Non-stick cookware set', 79.99, 3, 20);
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {TABLE_NAMES[3]} (user_id, order_date, order_price) 
VALUES 
    (1, NOW(), 1000.00),
    (2, NOW(), 52.99);
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {TABLE_NAMES[4]} (order_id, product_id, quantity, subtotal) 
VALUES 
    (1, 1, 2, 999.98),
    (2, 3, 3, 59.97);
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {TABLE_NAMES[5]} (order_id, transaction_date, transaction_status) 
VALUES 
    (1, NOW(), 'Pending'),
    (2, NOW(), 'Accepted');
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {TABLE_NAMES[6]} (product_id, user_id, rating, review_text) 
VALUES 
    (1, 1, 5, 'Great smartphone! Highly recommended.'),
    (3, 2, 4, 'Nice T-shirt, comfortable to wear.');
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {TABLE_NAMES[7]} (product_id, warehouse_name, stock_quantity, created_at) 
VALUES 
    (1, 'Warehouse A', 20, NOW()),
    (1, 'Warehouse B', 30, NOW()),
    (2, 'Warehouse A', 10, NOW());
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {TABLE_NAMES[8]} (order_id, status, ship_date, estimated_arrival_date, delivered_date) 
VALUES 
    (1, 'Shipped', NOW(), NOW() + INTERVAL '5 days', NOW() + INTERVAL '7 days'),
    (2, 'In Transit', NOW(), NOW() + INTERVAL '6 days', NULL);
                """

            cursor.execute(insert_statement)
            conn.commit()

            print(f"Inserted data!")

    except (Exception, psycopg2.Error) as error:
        print("Error inserting data:", error)

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

if __name__ == '__main__':

    create_database(DATABASE_CONFIG['DATABASE_NAME'])

    with connect_postgres(dbname=DATABASE_CONFIG['DATABASE_NAME']) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        create_tables(conn)
        select_data(conn)
        insert_mock_data(conn)
        select_data(conn)


        # cmd = f"UPDATE Products SET name = 'T shirt' WHERE product_id = 3;"
        # cursor = conn.cursor()
        # cursor.execute(cmd)
        # conn.commit()

        # delete_all_data(conn)
        # select_data(conn)
        # drop_all(conn)


        print('Done')
