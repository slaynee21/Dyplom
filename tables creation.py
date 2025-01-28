import pyodbc

def connect_to_db():
    try:
        conn = pyodbc.connect(
            # this unfortunatly cant be on github
        )
        print("Connection to database established successfully.")
        return conn
    except Exception as e:
        print(f"The error '{e}' occurred")
        return None

def create_tables(conn):
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INT IDENTITY(1,1) PRIMARY KEY,
                username NVARCHAR(50) NOT NULL,
                email NVARCHAR(50) NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS products (
                product_id INT IDENTITY(1,1) PRIMARY KEY,
                product_name NVARCHAR(100) NOT NULL,
                price DECIMAL(10, 2) NOT NULL,
                stock INT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS orders (
                order_id INT IDENTITY(1,1) PRIMARY KEY,
                user_id INT FOREIGN KEY REFERENCES users(user_id),
                product_id INT FOREIGN KEY REFERENCES products(product_id),
                quantity INT NOT NULL,
                total_price DECIMAL(10, 2) NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            """)
            conn.commit()
            print("Tables created successfully.")
        except Exception as e:
            print(f"The error '{e}' occurred")

def insert_test_data(conn):
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
            INSERT INTO users (username, email) VALUES
            ('alice', 'alice@example.com'),
            ('bob', 'bob@example.com'),
            ('charlie', 'charlie@example.com');

            INSERT INTO products (product_name, price, stock) VALUES
            ('Laptop', 999.99, 10),
            ('Smartphone', 499.99, 20),
            ('Tablet', 299.99, 15);

            INSERT INTO orders (user_id, product_id, quantity, total_price) VALUES
            (1, 1, 1, 999.99),
            (2, 2, 2, 999.98),
            (3, 3, 1, 299.99);
            """)
            conn.commit()
            print("Test data inserted successfully.")
        except Exception as e:
            print(f"The error '{e}' occurred")

def main():
    conn = connect_to_db()
    if conn:
        create_tables(conn)
        insert_test_data(conn)
        conn.close()
        print("Connection closed.")

if __name__ == "__main__":
    main()
