import pyodbc
import pandas as pd

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

def create_table_if_not_exists(conn):
    cursor = conn.cursor()
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='performance_data' AND xtype='U')
    BEGIN
        CREATE TABLE performance_data (
            timestamp DATETIME,
            pid INT,
            usename NVARCHAR(100),
            datname INT,
            query NVARCHAR(MAX),
            query_duration INT,
            state NVARCHAR(50),
            client_addr NVARCHAR(50),
            client_port INT
        );
    END
    """
    cursor.execute(create_table_query)
    conn.commit()

def create_logs_table_if_not_exists(conn):
    cursor = conn.cursor()
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='logs' AND xtype='U')
    BEGIN
        CREATE TABLE logs (
            timestamp DATETIME,
            log_time DATETIME,
            user_name NVARCHAR(100),
            database_name NVARCHAR(100),
            process_id INT,
            connection_from NVARCHAR(100),
            session_id INT,
            session_line_num INT,
            command_tag NVARCHAR(50),
            session_start_time DATETIME,
            virtual_transaction_id NVARCHAR(50),
            transaction_id INT,
            error_severity INT,
            sql_state_code NVARCHAR(10),
            message NVARCHAR(MAX),
            detail NVARCHAR(MAX),
            hint NVARCHAR(MAX),
            internal_query NVARCHAR(MAX),
            internal_query_pos INT,
            context NVARCHAR(100),
            query NVARCHAR(MAX),
            query_pos INT,
            location NVARCHAR(100),
            application_name NVARCHAR(100),
            session_status NVARCHAR(100),
            event NVARCHAR(100),
            user_agent NVARCHAR(255)
        );
    END
    """
    cursor.execute(create_table_query)
    conn.commit()

def collect_performance_data(conn):
    query = """
    SELECT
        GETDATE() AS timestamp,
        r.session_id AS pid,
        s.login_name AS usename,
        r.database_id AS datname,
        r.command AS query,
        DATEDIFF(SECOND, r.start_time, GETDATE()) AS query_duration,
        r.status AS state,
        c.client_net_address AS client_addr,
        c.client_tcp_port AS client_port
    FROM sys.dm_exec_requests r
    JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
    JOIN sys.dm_exec_connections c ON r.session_id = c.session_id
    WHERE r.status != 'sleeping'
    """
    df = pd.read_sql(query, conn)
    return df

def collect_logs(conn):
    query = """
    SELECT
        GETDATE() AS timestamp,
        r.start_time AS log_time,
        s.login_name AS user_name,
        r.database_id AS database_name,
        r.session_id AS process_id,
        c.client_net_address AS connection_from,
        r.session_id AS session_id,
        r.command AS command_tag,
        r.start_time AS session_start_time,
        r.status AS session_status,
        'login' AS event,  
        'user-agent-info' AS user_agent  
    FROM sys.dm_exec_requests r
    JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
    JOIN sys.dm_exec_connections c ON r.session_id = c.session_id
    WHERE r.status != 'sleeping'
    """
    df = pd.read_sql(query, conn)
    print(df.columns)
    return df

def save_to_db(data, table_name, conn):
    cursor = conn.cursor()
    for index, row in data.iterrows():
        placeholders = ", ".join(["?"] * len(row))
        columns = ", ".join(row.index)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, tuple(row))
    conn.commit()

def detect_high_frequency_requests(logs):
    request_counts = logs['event'].value_counts()
    high_freq_requests = request_counts[request_counts > 10]
    return high_freq_requests

def main():
    conn = connect_to_db()
    create_table_if_not_exists(conn)
    create_logs_table_if_not_exists(conn)
    performance_data = collect_performance_data(conn)
    save_to_db(performance_data, 'performance_data', conn)
    logs = collect_logs(conn)
    save_to_db(logs, 'logs', conn)
    high_freq_requests = detect_high_frequency_requests(logs)
    print(f"High frequency requests: {high_freq_requests}")
    conn.close()

if __name__ == "__main__":
    main()
