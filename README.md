# Distributed-Database-for-E-commerce

In the proposed E-commerce distributed database, key tables
include Users, Products, Categories, Orders, OrderDetails,
Transactions, Reviews, Inventory, and Shipping, each serving a
distinct purpose. The Users table stores registered user information,
while Products and Categories manage product details and
classifications. The Orders and OrderDetails tables track user
orders and their contents, and Transactions record payment details
for each order. The Reviews table captures user feedback on
products, Inventory manages stock levels and locations, and
Shipping monitors the delivery process for orders.

The project is divided into 6 parts and there is specific directory for each part including the deliverables.


## Setup Instructions

Before starting with Part 1, follow these steps to create and configure your primary database:

1. **Create Primary Database:**

initdb -D /tmp/primary_db/


2. **Configure `postgresql.conf` for Networking:**

- Edit `postgresql.conf` in `/tmp/primary_db/`:
  - Set `listen_addresses = '*'`
  - Set `port = 5433`
- Start the server:
  ```
  pg_ctl -D /tmp/primary_db start
  ```

3. **Create a Replication User:**
   create user repuser replication;
5. **AllowRemoteAccessin `pg_hba.conf`:**

- Add to `pg_hba.conf`:
  ```
  host all repuser 127.0.0.1/32 trust
  ```

5. **Restart the PrimaryServer:**

   pg_ctl -D /tmp/primary_db restart
6. **Configuring the Replica Server:**

   pg_basebackup -h localhost -U repuser --checkpoint=fast -D /tmp/replica_db/ -R --slot=some_name -C --port=5433
7. **Update `postgresql.conf` in Replica Server:**

   - In `/tmp/replica_db/`, set `port = 5434`
8. **Start Both Servers:**

   - Stop both servers if they are running.
   - Execute `server_start.py` in the Part 1 folder.

### Part 1: Design and Implementation of a Distributed Database System  \

Tools: PostgreSQL 
Tasks - Schema design, Table creation, Data distribution, Data insertion and retrieval.
It includes an Entity-Relation diagram to represent the proposed schema, attributes, keys and constraints.

### Part 2: Fragmentation and Replication Techniques \

Tools: PostgreSQL 
Tasks - Horizontal and vertical fragmentation, Replication model

### Part 3: Query Processing and Optimization Techniques \

Tools: PostgreSQL
Tasks - Query optimization (Analyze and optimize data retrieval), distributed indexing (query performance)

### Part 4: Distributed Transaction Management \

Tools: 
Tasks - Acid-compliant distributed transactions, concurrency control

### Part 5: Distributed NoSQL Database Systems Implementation \

Tools: MongoDB, Docker 
Tasks - Data schema, Basic CRUD operations, sample queries with data retrieval operations

### Part 6: A 3-Minute Video Demo \
