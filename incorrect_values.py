import random
from datetime import datetime, timedelta
import pyodbc

# Function to connect to the database
def connect_to_db():
    try:
        conn = pyodbc.connect(
            #this unfortunatly cant be on github
        )
        print("Connection to database established successfully.")
        return conn
    except Exception as e:
        print(f"The error '{e}' occurred")
        return None

# Function to generate synthetic data with errors
def generate_synthetic_errors(conn, num_entries=20):
    cursor = conn.cursor()

    # Add High-frequency requests errors
    for _ in range(num_entries):
        timestamp = datetime.now() - timedelta(seconds=random.randint(0, 86400))
        pid = random.randint(1, 1000)
        usename = f"user_{random.randint(1, 100)}"
        datname = random.randint(1, 10)
        query = f"SELECT * FROM table_{random.randint(1, 10)}"
        query_duration = round(random.uniform(0.1, 5.0), 3)  # in seconds
        state = random.choice(["active", "idle", "suspended"])
        client_addr = f"192.168.{random.randint(0, 255)}.{random.randint(0, 255)}"
        client_port = random.randint(1024, 65535)

        # Introduce high-frequency request error
        for _ in range(random.randint(10, 50)):  # multiple requests in a short time
            cursor.execute("""
                INSERT INTO performance_data (timestamp, pid, usename, datname, query, query_duration, state, client_addr, client_port)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (timestamp, pid, usename, datname, query, query_duration, state, client_addr, client_port))

    # Add Multiple failed login attempts errors
    events = ['login', 'logout', 'failed_login', 'query', 'update']
    user_agents = ['Mozilla/5.0', 'sqlmap', 'nikto', 'fuzz', 'PostmanRuntime']

    for _ in range(num_entries):
        timestamp = datetime.now() - timedelta(seconds=random.randint(0, 86400))
        log_time = datetime.now() - timedelta(seconds=random.randint(0, 86400))
        user_name = f"user_{random.randint(1, 100)}"
        database_name = f"db_{random.randint(1, 10)}"
        process_id = random.randint(1, 1000)
        connection_from = f"192.168.{random.randint(0, 255)}.{random.randint(0, 255)}"
        session_id = random.randint(1, 1000)
        session_line_num = random.randint(1, 100)
        command_tag = random.choice(['SELECT', 'INSERT', 'UPDATE', 'DELETE'])
        session_start_time = datetime.now() - timedelta(seconds=random.randint(0, 86400))
        virtual_transaction_id = f"{random.randint(1, 1000)}-{random.randint(1, 1000)}"
        transaction_id = random.randint(1, 1000)
        error_severity = random.randint(1, 25)
        sql_state_code = random.choice(['00000', '01000', '02000', '03000', '04000'])
        message = f"Message {random.randint(1, 100)}"
        detail = f"Detail {random.randint(1, 100)}"
        hint = f"Hint {random.randint(1, 100)}"
        internal_query = f"Internal query {random.randint(1, 100)}"
        internal_query_pos = random.randint(1, 100)
        context = f"Context {random.randint(1, 100)}"
        query = f"Query {random.randint(1, 100)}"
        query_pos = random.randint(1, 100)
        location = f"Location {random.randint(1, 100)}"
        application_name = f"App {random.randint(1, 100)}"
        session_status = random.choice(['active', 'idle', 'suspended'])
        event = random.choice(events)
        user_agent = random.choice(user_agents)

        # Introduce multiple failed login attempts error
        if event == 'failed_login':
            for _ in range(random.randint(5, 20)):  # multiple failed logins
                cursor.execute("""
                    INSERT INTO logs (timestamp, log_time, user_name, database_name, process_id, connection_from, session_id, session_line_num,
                    command_tag, session_start_time, virtual_transaction_id, transaction_id, error_severity, sql_state_code, message, detail, hint,
                    internal_query, internal_query_pos, context, query, query_pos, location, application_name, session_status, event, user_agent)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (timestamp, log_time, user_name, database_name, process_id, connection_from, session_id, session_line_num,
                      command_tag, session_start_time, virtual_transaction_id, transaction_id, error_severity, sql_state_code, message, detail, hint,
                      internal_query, internal_query_pos, context, query, query_pos, location, application_name, session_status, event, user_agent))

    conn.commit()
    print(f"Inserted {num_entries} erroneous entries into performance_data and logs tables.")

# Main function
def main():
    conn = connect_to_db()
    if conn:
        generate_synthetic_errors(conn, num_entries=20)

if __name__ == "__main__":
    main()
