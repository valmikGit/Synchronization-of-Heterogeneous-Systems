import psycopg2

# Connection parameters
host = "localhost"
port = 5432
def connect_to_postgresql(database="doshte", user="nande", password="050309"):
    conn=None
    cursor=None
    try:
        conn = psycopg2.connect(host=host,port=port,dbname=database,user=user,password=password)
        print("Connected to PostgreSQL successfully!")
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        print("Connection failed:", e)
        return conn, cursor

def disconnect_from_postgresql(conn):
    if cursor:
        cursor.close()
    if conn:
        conn.close()
        print("Disconnected from PostgreSQL successfully!")