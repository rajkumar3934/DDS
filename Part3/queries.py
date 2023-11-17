
import psycopg2
from psycopg2 import sql
from constants import POSTGRES_CONFIG, DATABASE_CONFIG

table_names = ["Users", "Categories", "Products", "Orders",
                       "OrderDetails", "Transactions", "Reviews",
                       "Inventory", "Shipping"]

# Connect to the PostgreSQL database
def connect_postgres(dbname):
    """Connect to the PostgreSQL using psycopg2 with default database
       Return the connection"""
    return psycopg2.connect(host=POSTGRES_CONFIG['HOST_NAME'], dbname=dbname, user=POSTGRES_CONFIG['USER_NAME'], password= POSTGRES_CONFIG['PASSWORD'], port=POSTGRES_CONFIG['PORT'])


# Function to create indexes
def create_indexes(conn):
    try:
        cursor = conn.cursor()
        
        # Creating an index on the 'product_id' column of the 'Orders' table
        cursor.execute(sql.SQL("CREATE INDEX IF NOT EXISTS idx_orders_product_id ON Orders (product_id);"))

        # Creating an index on the 'product_id' column of the 'Products' table
        cursor.execute(sql.SQL("CREATE INDEX IF NOT EXISTS idx_products_product_id ON Products (product_id);"))

        conn.commit()
        print("Indexes created successfully.")
    except Exception as e:
        print(f"Error creating indexes: {e}")
    finally:
        cursor.close()

# Function to execute a query
def execute_query(conn, query, message="Executing query..."):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()

        print(message)
        for row in result:
            print(row)

    except Exception as e:
        print(f"Error executing query: {e}")
    finally:
        cursor.close()

# Main function
def main():
    conn = connect_postgres()

    # Create indexes
    create_indexes(conn)

    # Distributed Query
    distributed_query = """
    SELECT o.order_id, p.product_name, p.category_id
    FROM Orders o
    JOIN Products p ON o.product_id = p.product_id
    WHERE p.category_id = 1;
    """
    execute_query(conn, distributed_query, "Executing Distributed Query...")

    # Optimized Distributed Query
    optimized_query = """
    SELECT o.order_id, p.product_name, p.category_id
    FROM (
        SELECT order_id, product_id
        FROM Orders
    ) o
    JOIN (
        SELECT product_id, product_name, category_id
        FROM Products
        WHERE category_id = 1
    ) p ON o.product_id = p.product_id;
    """
    execute_query(conn, optimized_query, "Executing Optimized Distributed Query...")

    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()
