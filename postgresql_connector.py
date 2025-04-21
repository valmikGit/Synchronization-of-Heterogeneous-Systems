import psycopg2
from psycopg2 import sql

class PostgreSQLHandler:
    def __init__(self, host: str = "localhost", port: int = 5432, database: str = "doshte", user: str = "nande", password: str = "050309"):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None
        self.connect()

    def connect(self):
        try:
            # Establish the connection
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            print(" Connected to PostgreSQL successfully!")
        except Exception as e:
            print(" Connection failed:", e)

    def set(self, table_name: str, pk: tuple, value: str, ts: int):
        try:
            # Construct the SQL query
            query = sql.SQL("""
                INSERT INTO {table} (pk, value, ts)
                VALUES (%s, %s, %s)
                ON CONFLICT (pk) 
                DO UPDATE SET value = EXCLUDED.value, ts = EXCLUDED.ts;
            """).format(table=sql.Identifier(table_name))
            
            # Execute the query
            self.cursor.execute(query, (pk, value, ts))
            self.connection.commit()
            print(f" Data inserted/updated into {table_name} successfully!")
        except Exception as e:
            print(" Set operation failed:", e)

    def get(self, table_name: str, pk: tuple):
        try:
            # Construct the SQL query
            query = sql.SQL("SELECT * FROM {table} WHERE pk = %s").format(table=sql.Identifier(table_name))
            
            # Execute the query
            self.cursor.execute(query, (pk,))
            result = self.cursor.fetchone()
            
            if result:
                print(f" Found data: {result}")
                return result
            else:
                print(" No data found for the given pk.")
                return None
        except Exception as e:
            print(" Get operation failed:", e)
            return None

    def disconnect(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            print(" Disconnected from PostgreSQL.")
        except Exception as e:
            print("Disconnection failed:", e)