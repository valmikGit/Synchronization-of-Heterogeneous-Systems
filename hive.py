import os
import subprocess
import time
from pyhive import hive
import csv
import uuid

class Hive:
    table_name = ""
    temp_table = ""

    student_id = ""
    course_id = ""

    current_directory = ""

    def __init__(self, table_name, host, port, username, auth, password):
        folder_name = 'mytables'
        current_directory = os.getcwd()
        folder_path = os.path.join(current_directory, folder_name)

        if not os.path.exists(folder_path):
            os.mkdir(folder_path)
            print(f"Folder '{folder_name}' created in {current_directory}")
        else:
            print(f"Folder '{folder_name}' already exists.")

        self.current_directory = os.getcwd()

        self.table_name = table_name
        self.temp_table = table_name + "_temp"

        hive_home = os.environ.get('HIVE_HOME')
        if not hive_home:
            print("HIVE_HOME is not set.")
            exit(1)

        print(f"HIVE_HOME is set to: {hive_home}")
        hive_server_cmd = os.path.join(hive_home, "bin", "hiveserver2")

        print("Starting HiveServer2...")
        self.hive_server_process = subprocess.Popen(
            hive_server_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        time.sleep(10)

        try:
            self.conn = hive.Connection(
                host=host,
                port=port,
                username=username,
                auth=auth,
                password=password
            )
            self.cursor = self.conn.cursor()
            self.cursor.execute("SET hive.exec.dynamic.partition=true")
            self.cursor.execute("SET hive.exec.dynamic.partition.mode=nonstrict")
        except Exception as e:
            print(f"Error connecting to Hive: {e}")
            self.destroy()

    def create_table(self):
        self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")

        self.cursor.execute(f"""
            CREATE TABLE {self.table_name} (
                grade STRING
            )
            PARTITIONED BY (student_id STRING, course_id STRING)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            LOCATION '${self.current_directory}/mytables'
        """)
        print(f"Partitioned table '{self.table_name}' created.")

    def load_data(self, grade = None):
        temp_filename = f"temp_{uuid.uuid4().hex}.csv"

        try:
            with open(temp_filename, mode='w', newline='') as file:
                if grade:
                    writer = csv.writer(file)
                    writer.writerow([grade])  
                else:
                    pass

            load_query = f"""
                LOAD DATA LOCAL INPATH '{os.path.abspath(temp_filename)}' 
                OVERWRITE INTO TABLE {self.table_name}
                PARTITION (student_id='{self.student_id}', course_id='{self.course_id}')
            """
            self.cursor.execute(load_query)
        
        finally:
            if os.path.exists(temp_filename):
                os.remove(temp_filename)

    def insert_data(self, pk, grade):
        self.student_id, self.course_id = pk
        self.load_data(grade)

        print(f"Data inserted into table '{self.table_name}'.")

    def delete_data(self, pk): 
        self.student_id, self.course_id = pk  
        self.load_data()

        print(f"Data deleted from table '{self.table_name}'.")

    def select_data(self, studentId=None, courseId=None):
        try:
            if studentId and courseId:
                query = f"SELECT * FROM {self.table_name} WHERE student_id = %s AND course_id = %s"
                self.cursor.execute(query, (studentId, courseId))
            elif studentId:
                query = f"SELECT * FROM {self.table_name} WHERE student_id = %s"
                self.cursor.execute(query, (studentId,))
            elif courseId:
                query = f"SELECT * FROM {self.table_name} WHERE course_id = %s"
                self.cursor.execute(query, (courseId,))
            else:
                query = f"SELECT * FROM {self.table_name}"
                self.cursor.execute(query)

            rows = self.cursor.fetchall()

            if not rows:
                print(f"No data found in table '{self.table_name}'")
            else:
                print(f"Data from table '{self.table_name}':")
                for row in rows:
                    print(row)

        except hive.Error as e:
            print(f"Hive error: {e}")

    def update_data(self, pk, grade):
        self.student_id, self.course_id = pk
        self.load_data(grade)

    def destroy(self):
        try:
            if hasattr(self, 'cursor'):
                self.cursor.close()
            if hasattr(self, 'conn'):
                self.conn.close()
        except:
            pass
        if hasattr(self, 'hive_server_process') and self.hive_server_process:
            self.hive_server_process.terminate()
            print("🛑 HiveServer2 stopped.")

    def __del__(self):
        self.destroy()

if __name__ == "__main__":
    hive_instance = Hive("student_grades", "localhost", 10000, "prat", "CUSTOM", "pc02@December")
    hive_instance.create_table()
    hive_instance.insert_data(("IMT2023001", "CSC101"), "A")
    hive_instance.select_data()
    hive_instance.insert_data(("IMT2023001", "CSC102"), "B")
    hive_instance.select_data()
    hive_instance.insert_data(("IMT2023002", "CSC101"), "C")
    hive_instance.select_data()
    hive_instance.update_data(("IMT2023001", "CSC101"), "B")
    hive_instance.select_data()
    hive_instance.delete_data(("IMT2023001", "CSC101"))
    hive_instance.select_data()
    hive_instance.delete_data(("IMT2023001", "CSC102"))
    hive_instance.delete_data(("IMT2023001", "CSC102"))
    hive_instance.select_data()
    hive_instance.__del__()