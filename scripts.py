import pandas as pd
import pyodbc
import matplotlib.pyplot as plt
import seaborn as sns
import json
from sqlalchemy import create_engine
from scipy.stats import zscore


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

conn = connect_to_db()

def get_connection():
    engine = create_engine(#this unfortunatly cant be on github)
    return engine

engine = get_connection()

performance_data = pd.read_sql("SELECT * FROM performance_data", engine)
logs = pd.read_sql("SELECT * FROM logs", engine)

print("Performance Data before dropping NA:")
print(performance_data.head())
print(performance_data.info())
print("Logs before dropping NA:")
print(logs.head())
print(logs.info())

print("Columns in logs DataFrame:", logs.columns)

performance_data = performance_data.dropna(subset=['query_duration'])
logs = logs.dropna()

print("Performance Data after dropping NA:")
print(performance_data.head())
print(performance_data.info())

# Check for data availability to plot
if not performance_data.empty:
    sns.histplot(performance_data['query_duration'], bins=50)
    plt.title('Query Duration Distribution')
    plt.xlabel('Duration (seconds)')
    plt.ylabel('Frequency')
    plt.show()
else:
    print("No valid data for query_duration to plot.")

def create_analysis_table_if_not_exists(conn):
    cursor = conn.cursor()
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='analysis_results' AND xtype='U')
    BEGIN
        CREATE TABLE analysis_results (
            id INT IDENTITY(1,1) PRIMARY KEY,
            analysis_type NVARCHAR(50),
            result NVARCHAR(MAX),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    END
    """
    cursor.execute(create_table_query)
    conn.commit()

create_analysis_table_if_not_exists(conn)

def save_analysis_result(conn, analysis_type, result):
    cursor = conn.cursor()
    sql = "INSERT INTO analysis_results (analysis_type, result) VALUES (?, ?)"
    cursor.execute(sql, (analysis_type, result))
    conn.commit()


def save_to_excel(data, sheet_name, file_name='analysis_results.xlsx'):
    try:
        with pd.ExcelWriter(file_name, engine='openpyxl', mode='a') as writer:
            data.to_excel(writer, sheet_name=sheet_name, index=False)
    except FileNotFoundError:
        with pd.ExcelWriter(file_name, engine='openpyxl', mode='w') as writer:
            data.to_excel(writer, sheet_name=sheet_name, index=False)


result = "Query duration distribution analyzed."
save_analysis_result(conn, 'query_duration_distribution', result)

def detect_anomalies(performance_data, logs):
    # Script 1: Long Queries
    long_queries = performance_data[performance_data['query_duration'] > performance_data['query_duration'].quantile(0.95)]
    print(f"Long Queries (95th percentile): {long_queries}")

    # Script 2: Unusual IP Addresses
    common_ips = performance_data['client_addr'].value_counts().nlargest(10).index
    unusual_ips = performance_data[~performance_data['client_addr'].isin(common_ips)]
    print(f"Unusual IP Addresses: {unusual_ips['client_addr'].unique()}")

    return long_queries, unusual_ips

long_queries, unusual_ips = detect_anomalies(performance_data, logs)

# Save analysis results
save_analysis_result(conn, 'long_queries', long_queries.to_json(orient='records'))

# Save analysis results to Excel
# save_to_excel(long_queries, 'Long Queries')
# save_to_excel(unusual_ips, 'Unusual IPs')
# Convert list of IP addresses to JSON
unusual_ips_json = json.dumps(list(unusual_ips['client_addr'].unique()))
save_analysis_result(conn, 'unusual_ips', unusual_ips_json)

# Long queries visualization
if not long_queries.empty:
    sns.scatterplot(data=long_queries, x='timestamp', y='query_duration')
    plt.title('Long Queries')
    plt.xlabel('Time')
    plt.ylabel('Duration (seconds)')
    plt.show()



def detect_high_frequency_requests(logs):
    # Script 3: High-frequency requests from a single IP
    if 'connection_from' in logs.columns:
        request_counts = logs['connection_from'].value_counts()
        high_freq_requests = request_counts[request_counts > request_counts.quantile(0.95)]
        print(f"High-frequency requests: {high_freq_requests}")
        save_to_excel(high_freq_requests.reset_index(), 'High Frequency Requests')
        return high_freq_requests
    else:
        print("Column 'connection_from' not found in logs DataFrame.")
        return pd.Series()

def detect_failed_logins(logs):
    # Script 4: Multiple failed login attempts
    if 'event' in logs.columns:
        failed_logins = logs[logs['event'] == 'failed_login']
        failed_login_counts = failed_logins['connection_from'].value_counts()
        frequent_failed_logins = failed_login_counts[failed_login_counts > failed_login_counts.quantile(0.95)]
        print(f"Multiple failed login attempts: {frequent_failed_logins}")
        save_to_excel(frequent_failed_logins.reset_index(), 'Failed Logins')
        return frequent_failed_logins
    else:
        print("Column 'event' not found in logs DataFrame.")
        return pd.Series()

def detect_suspicious_user_agents(logs):
    # Script 5 Suspicious User-Agent
    if 'user_agent' in logs.columns:
        suspicious_agents = logs[logs['user_agent'].str.contains('sqlmap|nikto|fuzz', na=False)]
        print(f"Suspicious User-Agent: {suspicious_agents}")
        save_to_excel(suspicious_agents, 'Suspicious User Agents')
        return suspicious_agents
    else:
        print("Column 'user_agent' not found in logs DataFrame.")
        return pd.DataFrame()

def detect_anomalies_z_score(performance_data, threshold=3):
    # Calculate Z-Score for query_duration
    performance_data['z_score'] = zscore(performance_data['query_duration'])
    anomalies = performance_data[performance_data['z_score'].abs() > threshold]
    print(f"Anomalous queries by Z-Score: {anomalies}")
    save_to_excel(anomalies, 'Z-Score Anomalies')
    return anomalies

def analyze_user_behavior(logs):
    user_activity = logs.groupby('user_name').size()
    user_activity_zscore = zscore(user_activity)
    unusual_activity = user_activity[user_activity_zscore > 3]

    print(f"Unusual User Activity: {unusual_activity}")
    save_to_excel(unusual_activity.reset_index(), 'Unusual User Activity')
    return unusual_activity



high_freq_requests = detect_high_frequency_requests(logs)
frequent_failed_logins = detect_failed_logins(logs)
suspicious_user_agents = detect_suspicious_user_agents(logs)
z_score_anomalies = detect_anomalies_z_score(performance_data)
unusual_activity = analyze_user_behavior(logs)



# Save analysis results
if not high_freq_requests.empty:
    save_analysis_result(conn, 'high_freq_requests', high_freq_requests.to_json(orient='records'))
if not frequent_failed_logins.empty:
    save_analysis_result(conn, 'frequent_failed_logins', frequent_failed_logins.to_json(orient='records'))
if not suspicious_user_agents.empty:
    save_analysis_result(conn, 'suspicious_user_agents', suspicious_user_agents.to_json(orient='records'))
if not z_score_anomalies.empty:
    save_analysis_result(conn, 'z_score_anomalies', z_score_anomalies.to_json(orient='records'))

# Zscore anomaly visualization
if not z_score_anomalies.empty:
    sns.scatterplot(data=z_score_anomalies, x='timestamp', y='query_duration', hue='z_score', palette='coolwarm', legend=False)
    plt.title('Z-Score Anomalies')
    plt.xlabel('Time')
    plt.ylabel('Duration (seconds)')
    plt.show()

