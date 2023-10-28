from constants import POSTGRES_CONFIG, DATABASE_CONFIG
from random import random
import psycopg2
import random
from datetime import date, timedelta
from tabulate import tabulate
#from psycopg2 import extensions

table_names = ["Users", "Categories", "Products", "Orders", 
                       "OrderDetails", "Transactions", "Reviews", 
                       "Inventory", "Shipping"]

def connect_postgres(dbname):
    """Connect to the PostgreSQL using psycopg2 with default database
       Return the connection"""
    return psycopg2.connect(host=POSTGRES_CONFIG['HOST_NAME'], dbname=dbname, user=POSTGRES_CONFIG['USER_NAME'], password= POSTGRES_CONFIG['PASSWORD'], port=POSTGRES_CONFIG['PORT'])



def demonstrate_H_partition(conn):
    print("\n---------HORIZONTAL PARTITIONING ------------\n")
    try:
        cursor = conn.cursor()
        if conn and cursor:
            insert_statement = f"""
                INSERT INTO {table_names[0]} (username, first_name, last_name, email, password, address, phone_number, registration_date) 
VALUES 
    ('user3', 'first3', 'last3', 'user3@example.com', 'password3', '789 Tmepe St, City3', '123-456-7890', '2014-01-20 00:00:00'),
    ('user4', 'first4', 'last4', 'user4@example.com', 'password4', '248 Dorsey St, City4', '234-567-8901', '2018-05-10 00:00:00');
                """

            cursor.execute(insert_statement)

            conn.commit()

            cursor.execute(f"SELECT * FROM {table_names[0]};")
            rows = cursor.fetchall()
            headers = [desc[0] for desc in cursor.description]
            print(f"Table: {table_names[0]}")
            print(tabulate(rows, headers=headers, tablefmt="grid"))
            print("\n" + "-" * 50 + "\n")  # Separator between tables
            cursor.execute(f"SELECT * FROM Users_Old;")
            rows = cursor.fetchall()
            headers = [desc[0] for desc in cursor.description]
            print(f"Table: Users_Old")
            print(tabulate(rows, headers=headers, tablefmt="grid"))
            print("\n" + "-" * 50 + "\n")  # Separator between tables
            cursor.execute(f"SELECT * FROM Users_New;")
            rows = cursor.fetchall()
            headers = [desc[0] for desc in cursor.description]
            print(f"Table: Users_New")
            print(tabulate(rows, headers=headers, tablefmt="grid"))
            print("\n" + "-" * 50 + "\n")  # Separator between tables

            cursor.execute(f"SELECT tableoid::regclass,* FROM users;")
            rows = cursor.fetchall()
            headers = [desc[0] for desc in cursor.description]
            print(f"Table: Users showing partitions")
            print(tabulate(rows, headers=headers, tablefmt="grid"))
            print("\n" + "-" * 50 + "\n")  # Separator between tables

    except (Exception, psycopg2.Error) as error:
        print("Error:", error)


def vertical_partitioning(conn):
    try:
        cursor = conn.cursor()
        # Creating partition tables
        cursor.execute("CREATE TABLE IF NOT EXISTS Products_Info AS SELECT product_id, product_name, description, product_price, category_id, product_quantity FROM Products;")
        cursor.execute("CREATE TABLE IF NOT EXISTS Products_Details AS SELECT product_id, created_at, updated_at FROM Products;")

        conn.commit()

        print("Tables with vertical partitions created successfully!")

    except (Exception, psycopg2.Error) as error:
        print("Error creating partitions:", error)


def demonstrate_V_partition(conn):
    print("\n---------VERTICAL PARTITIONING ------------\n")
    try:
        cursor = conn.cursor()
        if conn and cursor:
            insert_statement = f"""
                INSERT INTO {table_names[2]} (product_name, description, product_price, category_id, product_quantity) 
VALUES 
    ('Jeans', 'Denim Bottomwear', 89.99, 2, 25)
                """
            cursor.execute(insert_statement)

            conn.commit()

            cursor.execute(f"SELECT * FROM {table_names[2]};")
            rows = cursor.fetchall()
            headers = [desc[0] for desc in cursor.description]
            print(f"Table: {table_names[2]}")
            print(tabulate(rows, headers=headers, tablefmt="grid"))
            print("\n" + "-" * 50 + "\n")  # Separator between tables
            cursor.execute(f"SELECT * FROM Products_Info;")
            rows = cursor.fetchall()
            headers = [desc[0] for desc in cursor.description]
            print(f"Table: Products_Info")
            print(tabulate(rows, headers=headers, tablefmt="grid"))
            print("\n" + "-" * 50 + "\n")  # Separator between tables
            cursor.execute(f"SELECT * FROM Products_Details;")
            rows = cursor.fetchall()
            headers = [desc[0] for desc in cursor.description]
            print(f"Table: Products_Details")
            print(tabulate(rows, headers=headers, tablefmt="grid"))
            print("\n" + "-" * 50 + "\n")  # Separator between tables

    except (Exception, psycopg2.Error) as error:
        print("Error:", error)





if __name__ == '__main__':

    with connect_postgres(dbname=DATABASE_CONFIG['DATABASE_NAME']) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        demonstrate_H_partition(conn)
        vertical_partitioning(conn)
        demonstrate_V_partition(conn)


        # cmd = f"UPDATE Products SET name = 'T shirt' WHERE product_id = 3;"
        # cursor = conn.cursor()
        # cursor.execute(cmd)
        # conn.commit()

        # delete_all_data(conn)
        # select_data(conn)
        # drop_all(conn)


        print('Done')

