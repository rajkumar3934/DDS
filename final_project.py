
from random import random

import psycopg2
import random
from datetime import date, timedelta
from tabulate import tabulate
#from psycopg2 import extensions

DATABASE_NAME = "e_commerce"
table_names = ["Users", "Categories", "Products", "Orders", 
                       "OrderDetails", "Transactions", "Reviews", 
                       "Inventory", "Shipping"]


def connect_postgres(dbname='postgres'):
    try:
        # Defining parameters
        user = 'postgres'
        #dbname = dbname
        host = 'localhost'
        password = ''

        # Connecting with PostgreSQL database
        connection = psycopg2.connect(
            user=user,
            dbname=dbname,
            host=host,
            password=password
        )

        # Cursor object to execute SQL queries
        cursor = connection.cursor()

        print("Connected with postgres!")

        # Return the connection
        return connection

    except (Exception, psycopg2.Error) as error:
        print("Error connecting to PostgreSQL:", error)
        return None

def create_database(dbname):
    connection = None
    try:
        connection = connect_postgres()
        if connection:
            connection.autocommit = True
            cursor = connection.cursor()
            create_db = "CREATE DATABASE " + DATABASE_NAME + ";"

            # Creating the database
            cursor.execute(create_db)
            connection.commit()

            print(f"Database {DATABASE_NAME} created!")
            print("")
            print("---------------------------------------")
            print("")

    except (Exception, psycopg2.Error) as error:
        print("Error creating database:", error)

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def create_tables(conn):
    try:
        cur = conn.cursor()
        if conn and cur:
            create_table_command = f"""
            CREATE TABLE {table_names[0]} (
    user_id SERIAL,
    username TEXT NOT NULL,
    email TEXT NOT NULL,
    password TEXT NOT NULL,
    address TEXT,
    phone_number TEXT,
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, registration_date)
)
PARTITION BY RANGE (registration_date);
            """
            # Executing the above SQL command 
            cur.execute(create_table_command)
            print(f"{table_names[0]} created")

            horizontal_partitioning(conn)

            create_table_command = f"""
            CREATE TABLE {table_names[1]} (
    category_id SERIAL PRIMARY KEY,
    category_name TEXT NOT NULL
);
            """
            # Executing the above SQL command
            cur.execute(create_table_command)
            print(f"{table_names[1]} created")

            create_table_command = f"""
            CREATE TABLE {table_names[2]} (
    product_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    price DECIMAL NOT NULL,
    category_id INT REFERENCES Categories(category_id),
    stock_quantity INT DEFAULT 0,
    created_at date DEFAULT CURRENT_DATE,
    updated_at date DEFAULT CURRENT_DATE
);
            """
            # Executing the above SQL command
            cur.execute(create_table_command)
            print(f"{table_names[2]} created")

            create_table_command = f"""
            CREATE TABLE {table_names[3]} (
    order_id SERIAL PRIMARY KEY,
    user_id INT,
    user_registration_date TIMESTAMP,
    order_date date DEFAULT CURRENT_DATE,
    FOREIGN KEY (user_id, user_registration_date) REFERENCES Users(user_id, registration_date)
);
            """
            # Executing the above SQL command
            cur.execute(create_table_command)
            print(f"{table_names[3]} created")

            create_table_command = f"""
            CREATE TABLE {table_names[4]} (
    order_detail_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES Orders(order_id),
    product_id INT REFERENCES Products(product_id),
    quantity INT,
    subtotal DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
            """
            # Executing the above SQL command
            cur.execute(create_table_command)
            print(f"{table_names[4]} created")

            create_table_command = f"""
            CREATE TABLE {table_names[5]} (
    transaction_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES Orders(order_id),
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payment_status BOOLEAN DEFAULT FALSE 
);
            """
            # Executing the above SQL command
            cur.execute(create_table_command)
            print(f"{table_names[5]} created")

            create_table_command = f"""
            CREATE TABLE {table_names[6]} (
    review_id SERIAL PRIMARY KEY,
    product_id INT REFERENCES Products(product_id),
    user_id INT,
    user_registration_date TIMESTAMP,
    rating INT CHECK (rating >= 1 AND rating <= 5),
    review_text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id, user_registration_date) REFERENCES Users(user_id, registration_date)
);
            """
            # Executing the above SQL command
            cur.execute(create_table_command)
            print(f"{table_names[6]} created")

            create_table_command = f"""
            CREATE TABLE {table_names[7]} (
    product_id INT REFERENCES Products(product_id),
    warehouse_location TEXT,
    stock_quantity INT DEFAULT 0,
    PRIMARY KEY (product_id, warehouse_location)
);
            """
            # Executing the above SQL command
            cur.execute(create_table_command)
            print(f"{table_names[7]} created")

            create_table_command = f"""
            CREATE TABLE {table_names[8]} (
    order_id INT REFERENCES Orders(order_id),
    status TEXT,
    ship_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    estimated_arrival_date TIMESTAMP,
    actual_arrival_date TIMESTAMP,
    PRIMARY KEY (order_id)
);
            """
            # Executing the above SQL command
            cur.execute(create_table_command)
            print(f"{table_names[8]} created")

            conn.commit()

            print("Tables created successfully!")

    except (Exception, psycopg2.Error) as error:
        print("Error creating tables:", error)



def insert_mock_data(conn):
    try:
        cursor = conn.cursor()
        if conn and cursor:
            insert_statement = f"""
                INSERT INTO {table_names[0]} (username, email, password, address, phone_number) 
VALUES 
    ('user1', 'user1@example.com', 'password1', '123 Main St, City1', '123-456-7890'),
    ('user2', 'user2@example.com', 'password2', '456 Elm St, City2', '234-567-8901'),
    ('user3', 'user3@example.com', 'password3', '789 Oak St, City3', '345-678-9012');
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {table_names[1]} (category_name) 
VALUES 
    ('Electronics'),
    ('Clothing'),
    ('Home & Kitchen');
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {table_names[2]} (name, description, price, category_id, stock_quantity) 
VALUES 
    ('Smartphone', 'Latest smartphone model', 499.99, 1, 50),
    ('Laptop', 'High-performance laptop', 899.99, 1, 30),
    ('T-shirt', 'Cotton T-shirt', 19.99, 2, 100),
    ('Dress Shirt', 'Formal dress shirt', 29.99, 2, 80),
    ('Cookware Set', 'Non-stick cookware set', 79.99, 3, 20);
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {table_names[3]} (user_id, order_date) 
VALUES 
    (1, NOW()),
    (2, NOW());
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {table_names[4]} (order_id, product_id, quantity, subtotal) 
VALUES 
    (1, 1, 2, 999.98),
    (2, 3, 3, 59.97);
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {table_names[5]} (order_id, transaction_date, payment_status) 
VALUES 
    (1, NOW(), TRUE),
    (2, NOW(), TRUE);
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {table_names[6]} (product_id, user_id, rating, review_text) 
VALUES 
    (1, 1, 5, 'Great smartphone! Highly recommended.'),
    (3, 2, 4, 'Nice T-shirt, comfortable to wear.');
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {table_names[7]} (product_id, warehouse_location, stock_quantity) 
VALUES 
    (1, 'Warehouse A', 20),
    (1, 'Warehouse B', 30),
    (2, 'Warehouse A', 10);
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {table_names[8]} (order_id, status, ship_date, estimated_arrival_date, actual_arrival_date) 
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
            for table_name in table_names:
              cursor.execute(f"SELECT * FROM {table_name};")
              rows = cursor.fetchall()
              headers = [desc[0] for desc in cursor.description]
              print(f"Data from Table: {table_name}")
              print(tabulate(rows, headers=headers, tablefmt="grid"))
              print("\n" + "-" * 50 + "\n")  # Separator between tables

    except (Exception, psycopg2.Error) as error:
        print("Error selecting data:", error)


def horizontal_partitioning_copy(conn):
    try:
        cursor = conn.cursor()
        # Creating partition tables
        cursor.execute("SELECT category_id, category_name FROM Categories;")
        categories = cursor.fetchall()
        for category in categories:
            category_id, category_name = category
            table_name = "Products_" + category_name.replace(" ", "_")
            # create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM Products WHERE category_id = %s;"
            partition_command = f"CREATE TABLE IF NOT EXISTS {table_name} PARTITION OF Products FOR VALUES IN ('{category_id}');"
            # cursor.execute(create_table_query, (category_id,))
            

            cursor.execute(partition_command)
            conn.commit()

        print("Tables with partitions created successfully!")

    except (Exception, psycopg2.Error) as error:
        print("Error creating partitions:", error)

def horizontal_partitioning(conn):
    try:
        cursor = conn.cursor()
        # Creating partition tables
        cursor.execute(f"""CREATE TABLE Users_Old PARTITION OF Users
                            FOR VALUES FROM ('2010-01-01 00:00:00') TO ('2015-12-31 00:00:00');""")

        cursor.execute(f"""CREATE TABLE Users_New PARTITION OF Users
                            FOR VALUES FROM ('2016-01-01 00:00:00') TO ('2023-12-31 00:00:00');""")
        conn.commit()

        print("Tables with horizontal partitions created successfully!")

    except (Exception, psycopg2.Error) as error:
        print("Error creating partitions:", error)


def demonstrate_H_partition(conn):
    print("\n---------HORIZONTAL PARTITIONING ------------\n")
    try:
        cursor = conn.cursor()
        if conn and cursor:
            insert_statement = f"""
                INSERT INTO {table_names[0]} (username, email, password, address, phone_number, registration_date) 
VALUES 
    ('user3', 'user3@example.com', 'password3', '789 Tmepe St, City3', '123-456-7890', '2014-01-20 00:00:00'),
    ('user4', 'user4@example.com', 'password4', '248 Dorsey St, City4', '234-567-8901', '2018-05-10 00:00:00');
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

            # cursor.execute("SELECT category_id, category_name FROM Categories;")
            # categories = cursor.fetchall()

            # for category in categories:
            #   category_id, category_name = category
            #   table_name = "Products_" + category_name.replace(" ", "_")
            #   cursor.execute(f"SELECT * FROM {table_name};")
            #   rows = cursor.fetchall()
            #   headers = [desc[0] for desc in cursor.description]
            #   print(f"Table: {table_name}")
            #   print(tabulate(rows, headers=headers, tablefmt="grid"))
            #   print("\n" + "-" * 50 + "\n")  # Separator between tables

    except (Exception, psycopg2.Error) as error:
        print("Error:", error)


def vertical_partitioning(conn):
    try:
        cursor = conn.cursor()
        # Creating partition tables
        cursor.execute("CREATE TABLE IF NOT EXISTS Products_Info AS SELECT product_id, name, price, category_id, stock_quantity FROM Products;")
        cursor.execute("CREATE TABLE IF NOT EXISTS Products_Details AS SELECT product_id, description, created_at, updated_at FROM Products;")

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
                INSERT INTO {table_names[2]} (name, description, price, category_id, stock_quantity) 
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




def delete_all_data(conn):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM Transactions;")
    cursor.execute("DELETE FROM Reviews;")
    cursor.execute("DELETE FROM Inventory;")
    cursor.execute("DELETE FROM Shipping;")
    cursor.execute("DELETE FROM OrderDetails;")
    cursor.execute("DELETE FROM Orders;")
    cursor.execute("DELETE FROM Users;")
    cursor.execute("DELETE FROM Products;")
    cursor.execute("DELETE FROM Categories;")
    conn.commit()

def drop_all(conn):
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS Transactions;")
    cursor.execute("DROP TABLE IF EXISTS Reviews;")
    cursor.execute("DROP TABLE IF EXISTS Inventory;")
    cursor.execute("DROP TABLE IF EXISTS Shipping;")
    cursor.execute("DROP TABLE IF EXISTS OrderDetails;")
    cursor.execute("DROP TABLE IF EXISTS Orders;")
    cursor.execute("DROP TABLE IF EXISTS Users;")
    cursor.execute("DROP TABLE IF EXISTS Products;")
    cursor.execute("DROP TABLE IF EXISTS Categories;")
    cursor.execute("DROP TABLE IF EXISTS Products_Info;")
    cursor.execute("DROP TABLE IF EXISTS Products_Details;")
    cursor.execute("DROP TABLE IF EXISTS Users_Old;")
    cursor.execute("DROP TABLE IF EXISTS Users_New;")
    conn.commit()

if __name__ == '__main__':

    create_database(DATABASE_NAME)

    with connect_postgres(dbname=DATABASE_NAME) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        create_tables(conn)
        select_data(conn)
        insert_mock_data(conn)
        select_data(conn)
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




