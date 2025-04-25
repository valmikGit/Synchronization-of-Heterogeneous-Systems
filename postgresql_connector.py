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

    def set(self, table: str, pk: tuple, value: str, ts: int):
        student_id, course_id = pk
        print(student_id, course_id, value, ts)

        update_query = f"""UPDATE {table} SET grade = %s WHERE "student-ID" = %s AND "course-id" = %s;"""

        self.cursor.execute(update_query, (value, student_id, course_id))

        if self.cursor.rowcount == 0:
            print("No matching row found to update.")
        elif self.cursor.rowcount == 1:
            print("Row updated successfully.")
        else:
            print("Multiple rows updated (unexpected).")
        self.connection.commit()



    def get(self, table_name: str, pk: tuple):
        try:
            # Define the composite PK columns for your table
            pk_columns = ("student-ID", "course-id")

            if len(pk) != len(pk_columns):
                raise ValueError(f"PK values count {len(pk)} doesn't match PK columns count {len(pk_columns)}.")

            # Construct the WHERE clause: "student_id = %s AND course_id = %s"
            where_clause = sql.SQL(" AND ").join(
                sql.Composed([sql.Identifier(col), sql.SQL(" = %s")]) for col in pk_columns
            )

            # SQL query: "SELECT * FROM table WHERE student_id = %s AND course_id = %s"
            query = sql.SQL("SELECT * FROM {table} WHERE {conditions}").format(
                table=sql.Identifier(table_name),
                conditions=where_clause
            )

            # Execute the query with the primary key values
            self.cursor.execute(query, pk)
            result = self.cursor.fetchone()

            if result:
                print(f"Found data: {result}")
                return result
            else:
                print("No data found for the given PK.")
                return None

        except Exception as e:
            print("Get operation failed:", e)
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