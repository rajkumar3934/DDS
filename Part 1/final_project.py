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


def create_tables(conn):
    try:
        cur = conn.cursor()
        if conn and cur:
            create_table_command = f"""
            CREATE TABLE IF NOT EXISTS {table_names[0]} (
    user_id SERIAL,
    username TEXT NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
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
            # print(f"{table_names[0]} created")

            create_partition_table_users(conn)

            create_table_command = f"""
            CREATE TABLE IF NOT EXISTS {table_names[1]} (
    category_id SERIAL PRIMARY KEY,
    category_name TEXT NOT NULL
);
            """
            # Executing the above SQL command
            cur.execute(create_table_command)
            # print(f"{table_names[1]} created")

            create_table_command = f"""
            CREATE TABLE IF NOT EXISTS {table_names[2]} (
    product_id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    description TEXT,
    product_price DECIMAL NOT NULL,
    category_id INT REFERENCES Categories(category_id),
    product_quantity INT DEFAULT 0,
    created_at date DEFAULT CURRENT_DATE,
    updated_at date DEFAULT CURRENT_DATE
);
            """
            # Executing the above SQL command
            cur.execute(create_table_command)
            # print(f"{table_names[2]} created")

            create_table_command = f"""
            CREATE TABLE IF NOT EXISTS {table_names[3]} (
    order_id SERIAL PRIMARY KEY,
    user_id INT,
    user_registration_date TIMESTAMP,
    order_price DECIMAL NOT NULL,
    order_date date DEFAULT CURRENT_DATE,
    FOREIGN KEY (user_id, user_registration_date) REFERENCES Users(user_id, registration_date)
);
            """
            # Executing the above SQL command
            cur.execute(create_table_command)
            # print(f"{table_names[3]} created")

            create_table_command = f"""
            CREATE TABLE IF NOT EXISTS {table_names[4]} (
    order_detail_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES Orders(order_id),
    product_id INT REFERENCES Products(product_id),
    quantity INT,
    subtotal DECIMAL
);
            """
            # Executing the above SQL command
            cur.execute(create_table_command)
            # print(f"{table_names[4]} created")

            create_table_command = f"""
            CREATE TABLE IF NOT EXISTS {table_names[5]} (
    transaction_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES Orders(order_id),
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    transaction_status TEXT
);
            """
            # Executing the above SQL command
            cur.execute(create_table_command)
            # print(f"{table_names[5]} created")

            create_table_command = f"""
            CREATE TABLE IF NOT EXISTS {table_names[6]} (
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
            # print(f"{table_names[6]} created")

            create_table_command = f"""
            CREATE TABLE IF NOT EXISTS {table_names[7]} (
    inventory_id SERIAL PRIMARY KEY,
    product_id INT REFERENCES Products(product_id),
    warehouse_name TEXT,
    stock_quantity INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
            """
            # Executing the above SQL command
            cur.execute(create_table_command)
            # print(f"{table_names[7]} created")

            create_table_command = f"""
            CREATE TABLE IF NOT EXISTS {table_names[8]} (
    shipping_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES Orders(order_id),
    status TEXT,
    ship_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    estimated_arrival_date TIMESTAMP,
    delivered_date TIMESTAMP
);
            """
            # Executing the above SQL command
            cur.execute(create_table_command)
            # print(f"{table_names[8]} created")

            conn.commit()

            print("Tables created successfully!")

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
                INSERT INTO {table_names[0]} (username, first_name, last_name, email, password, address, phone_number) 
VALUES 
    ('user1', 'first1', 'last1', 'user1@example.com', 'password1', '123 Main St, City1', '123-456-7890'),
    ('user2', 'first2', 'last2', 'user2@example.com', 'password2', '456 Elm St, City2', '234-567-8901'),
    ('user3', 'first3', 'last3', 'user3@example.com', 'password3', '789 Oak St, City3', '345-678-9012');
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
                INSERT INTO {table_names[2]} (product_name, description, product_price, category_id, product_quantity) 
VALUES 
    ('Smartphone', 'Latest smartphone model', 499.99, 1, 50),
    ('Laptop', 'High-performance laptop', 899.99, 1, 30),
    ('T-shirt', 'Cotton T-shirt', 19.99, 2, 100),
    ('Dress Shirt', 'Formal dress shirt', 29.99, 2, 80),
    ('Cookware Set', 'Non-stick cookware set', 79.99, 3, 20);
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {table_names[3]} (user_id, order_date, order_price) 
VALUES 
    (1, NOW(), 1000.00),
    (2, NOW(), 52.99);
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
                INSERT INTO {table_names[5]} (order_id, transaction_date, transaction_status) 
VALUES 
    (1, NOW(), 'Pending'),
    (2, NOW(), 'Accepted');
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
                INSERT INTO {table_names[7]} (product_id, warehouse_name, stock_quantity, created_at) 
VALUES 
    (1, 'Warehouse A', 20, NOW()),
    (1, 'Warehouse B', 30, NOW()),
    (2, 'Warehouse A', 10, NOW());
                """

            cursor.execute(insert_statement)

            insert_statement = f"""
                INSERT INTO {table_names[8]} (order_id, status, ship_date, estimated_arrival_date, delivered_date) 
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
