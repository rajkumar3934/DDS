from constants import POSTGRES_CONFIG, DATABASE_CONFIG, TABLE_NAMES, CREATE_QUERIES
from random import random
import psycopg2
import random
from datetime import date, timedelta
from tabulate import tabulate
from faker import Faker
import random
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
    fake = Faker()
    try:
        cursor = conn.cursor()
        if conn and cursor:
            for _ in range(50):
                username = fake.user_name()
                first_name = fake.first_name()
                last_name = fake.last_name()
                email = fake.email()
                password = fake.password()
                address = fake.address()
                phone_number = fake.phone_number()
                registration_date = fake.date_time_this_decade()
        
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAMES[0]} (username, first_name, last_name, email, password, address, phone_number, registration_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (username, first_name, last_name, email, password, address, phone_number, registration_date))


            categories = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports & Outdoors"]
            for category_name in categories:
                cursor.execute(f"""
                INSERT INTO {TABLE_NAMES[1]} (category_name) VALUES (%s)
                """, (category_name,))

            cat_ids = []
            cursor.execute("SELECT category_id FROM Categories")
            cat_ids = [row[0] for row in cursor.fetchall()]
            for _ in range(50):
                product_name = fake.word().capitalize()  
                description = fake.text(max_nb_chars=100)  
                product_price = round(random.uniform(5, 200), 2) 
                category_id = random.choice(cat_ids)
                product_quantity = random.randint(0, 100)  
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAMES[2]} (product_name, description, product_price, category_id, product_quantity)
                    VALUES (%s, %s, %s, %s, %s)
                """, (product_name, description, product_price, category_id, product_quantity))


            usrs = []
            cursor.execute("SELECT user_id, registration_date FROM Users")
            usrs = cursor.fetchall()

            for _ in range(50):
                user_id, registration_date = random.choice(usrs)
                order_price = round(random.uniform(10, 1000), 2) 
                order_date = fake.date_between(start_date=registration_date, end_date="today")
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAMES[3]} (user_id, user_registration_date, order_price, order_date)
                    VALUES (%s, %s, %s, %s)
                """, (user_id, registration_date, order_price, order_date))


            ord_ids = []
            cursor.execute("SELECT order_id FROM Orders")
            ord_ids = [row[0] for row in cursor.fetchall()]
            prod_ids = []
            cursor.execute("SELECT product_id FROM Products")
            prod_ids = [row[0] for row in cursor.fetchall()]
            for _ in range(50):
                order_id = random.choice(ord_ids)
                product_id = random.choice(prod_ids)
                quantity = random.randint(1, 10)  
                subtotal = round(random.uniform(10, 100), 2) 
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAMES[4]} (order_id, product_id, quantity, subtotal)
                    VALUES (%s, %s, %s, %s)
                """, (order_id, product_id, quantity, subtotal))


            for _ in range(50):
                order_id = random.choice(ord_ids)
                transaction_date = fake.date_time_between(start_date="-1y", end_date="now")
                transaction_status = random.choice(["Success", "Pending", "Failed"])
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAMES[5]} (order_id, transaction_date, transaction_status)
                    VALUES (%s, %s, %s)
                """, (order_id, transaction_date, transaction_status))


            for _ in range(50):
                product_id = random.choice(prod_ids)
                user_id, user_registration_date = random.choice(usrs)
                rating = random.randint(1, 5)
                review_text = fake.paragraph()
                created_at = fake.date_time_between(start_date="-365d", end_date="now")
        
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAMES[6]} (product_id, user_id, user_registration_date, rating, review_text, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (product_id, user_id, user_registration_date, rating, review_text, created_at))



            for _ in range(50):
                product_id = random.choice(prod_ids)
                warehouse_name = fake.company()
                stock_quantity = random.randint(0, 1000)
                created_at = fake.date_time_between(start_date="-30d", end_date="now")
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAMES[7]} (product_id, warehouse_name, stock_quantity, created_at)
                    VALUES (%s, %s, %s, %s)
                """, (product_id, warehouse_name, stock_quantity, created_at))


            for _ in range(1, 51):
                order_id = random.choice(ord_ids)
                status = random.choice(["Shipped", "Processing", "Delivered"])
                ship_date = fake.date_time_between(start_date="-30d", end_date="now")
                estimated_arrival_date = ship_date + timedelta(days=random.randint(1, 10))
                delivered_date = estimated_arrival_date + timedelta(days=random.randint(1, 5)) if status == "Delivered" else None
        
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAMES[8]} (order_id, status, ship_date, estimated_arrival_date, delivered_date)
                    VALUES (%s, %s, %s, %s, %s)
                """, (order_id, status, ship_date, estimated_arrival_date, delivered_date))


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
