# Postgres configuration
POSTGRES_CONFIG = {
    'HOST_NAME': 'localhost',
    'USER_NAME': 'postgres',
    'PASSWORD': 'postgres',
    'PORT': 5432
}

# Database information
DATABASE_CONFIG = {
    'DATABASE_NAME': "e_commerce"
}

# Tables Names
TABLE_NAMES = ["Users", "Categories", "Products", "Orders", "OrderDetails", "Transactions", "Reviews", "Inventory", "Shipping"]

USERS_QUERY = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAMES[0]} (
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

CATEGORIES_QUERY = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAMES[1]} (
    category_id SERIAL PRIMARY KEY,
    category_name TEXT NOT NULL
);
"""

PRODUCTS_QUERY = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAMES[2]} (
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

ORDERS_QUERY =  f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAMES[3]} (
    order_id SERIAL PRIMARY KEY,
    user_id INT,
    user_registration_date TIMESTAMP,
    order_price DECIMAL NOT NULL,
    order_date date DEFAULT CURRENT_DATE,
    FOREIGN KEY (user_id, user_registration_date) REFERENCES Users(user_id, registration_date)
);
"""

ORDER_DETAILS_QUERY = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAMES[4]} (
    order_detail_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES Orders(order_id),
    product_id INT REFERENCES Products(product_id),
    quantity INT,
    subtotal DECIMAL
);
"""

TRANSACTIONS_QUERY = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAMES[5]} (
    transaction_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES Orders(order_id),
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    transaction_status TEXT
);
"""

REVIEWS_QUERY = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAMES[6]} (
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

INVENTORY_QUERY = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAMES[7]} (
    inventory_id SERIAL PRIMARY KEY,
    product_id INT REFERENCES Products(product_id),
    warehouse_name TEXT,
    stock_quantity INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

SHIPPING_QUERY = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAMES[8]} (
    shipping_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES Orders(order_id),
    status TEXT,
    ship_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    estimated_arrival_date TIMESTAMP,
    delivered_date TIMESTAMP
);
"""

CREATE_QUERIES = {
    0: USERS_QUERY,
    1: CATEGORIES_QUERY,
    2: PRODUCTS_QUERY,
    3: ORDERS_QUERY,
    4: ORDER_DETAILS_QUERY,
    5: TRANSACTIONS_QUERY,
    6: REVIEWS_QUERY,
    7: INVENTORY_QUERY,
    8: SHIPPING_QUERY
}