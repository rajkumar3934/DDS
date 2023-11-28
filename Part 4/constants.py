# Postgres configuration
# Rightnow we stopped the AWS Aurora cluster due to pricing constraints, if needed we will restart for verification
POSTGRES_CONFIG = {
    'HOST_NAME': 'part-4.cluster-cbg6fltkvvl6.us-east-1.rds.amazonaws.com',
    'USER_NAME': 'postgres',
    'PASSWORD': 'database',
    'PORT': 5432
}

# Database information
DATABASE_CONFIG = {
    'DATABASE_NAME': "e_commerce"
}

# Tables Names
TABLE_NAMES = ["Orders", "OrderDetails", "Inventory"]



ORDERS_QUERY =  f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAMES[0]} (
                orders_id SERIAL PRIMARY KEY,
                users_id INTEGER NOT NULL,
                order_price NUMERIC(10, 2) NOT NULL,
                order_date DATE NOT NULL
            );
        """

ORDER_DETAILS_QUERY = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAMES[1]} (
                order_details_id SERIAL PRIMARY KEY,
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                subtotal NUMERIC(10, 2) NOT NULL
            );
        """

INVENTORY_QUERY = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAMES[2]} (
                inventory_id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL,
                warehouse_name VARCHAR(255) NOT NULL,
                stock_quantity INTEGER NOT NULL CHECK (stock_quantity >= 0)
            );
        """

INSERT_QUERY = f"""
            INSERT INTO {TABLE_NAMES[2]} (product_id, warehouse_name, stock_quantity)
            VALUES 
            (1, 'Main Warehouse', 100),
            (2, 'Main Warehouse', 150),
            (3, 'Secondary Warehouse', 200);
        """

CREATE_QUERIES = {
    0: ORDERS_QUERY,
    1: ORDER_DETAILS_QUERY,
    2: INVENTORY_QUERY,
    3: INSERT_QUERY
}