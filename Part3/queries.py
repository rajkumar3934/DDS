
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
    with conn.cursor() as cursor:
        try:
            # Index on category_id in the Products table
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_category_id ON Products (category_id);")

            # Index on product_id in the Inventory table
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_inventory_product_id ON Inventory (product_id);")

            conn.commit()
            print("Indexes created successfully.")
        except Exception as e:
            conn.rollback()
            print(f"Error creating indexes: {e}")
    print()

# Function to execute a query
def execute_query(conn, query, params=None, message="Executing query..."):
    with conn.cursor() as cursor:
        try:
            cursor.execute(query, params)
            result = cursor.fetchall()

            print(message)
            for row in result:
                print(row)
        except Exception as e:
            print(f"Error executing query: {e}")

    print()

# Main function
def main():
    conn = connect_postgres(DATABASE_CONFIG['DATABASE_NAME'])


     # Define the category_id for the query
    category_id = 1  # Example category ID

    # This query retrieves the details of all reviews for products in a specific category, including user details
    # Non-Optimized Query
    non_optimized_query = """
        SELECT *
        FROM Reviews r
        JOIN Users u ON r.user_id = u.user_id
        JOIN Products p ON r.product_id = p.product_id
        JOIN Categories c ON p.category_id = c.category_id
        WHERE c.category_name = 'Electronics'
        AND r.rating = (SELECT MAX(rating) FROM Reviews);
    """
    execute_query(conn, non_optimized_query, (category_id,), "Executing Non-Optimized Query#1...")

    # This query retrieves product information and associated reviews
    # Non-Optimized Query
    non_optimized_query = """
        SELECT *
        FROM Products p
        JOIN Reviews r ON p.product_id = r.product_id
        WHERE p.product_id IN (SELECT product_id FROM OrderDetails);
    """
    execute_query(conn, non_optimized_query, (category_id,), "Executing Non-Optimized Query#2...")

    # Create indexes for faster retrieval
    create_indexes(conn)

    # Optimized Query
    optimized_query = """
        SELECT r.review_text, r.rating, u.username, p.product_name
        FROM Reviews r
        INNER JOIN Users u ON r.user_id = u.user_id
        INNER JOIN Products p ON r.product_id = p.product_id
        INNER JOIN Categories c ON p.category_id = c.category_id
        WHERE c.category_name = 'Electronics'
        AND r.rating = (SELECT MAX(rating) FROM Reviews WHERE product_id = p.product_id);
    """
    execute_query(conn, optimized_query, (category_id,), "Executing Optimized Query#1...")

    # Optimized Query
    optimized_query = """
        SELECT p.product_name, p.product_price, r.review_text, r.rating
        FROM Products p
        INNER JOIN Reviews r ON p.product_id = r.product_id
        INNER JOIN OrderDetails od ON p.product_id = od.product_id
        WHERE EXISTS (
            SELECT 1
            FROM OrderDetails od2
            WHERE od2.product_id = p.product_id
        )
    """
    execute_query(conn, optimized_query, (category_id,), "Executing Optimized Query#2...")

    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()
