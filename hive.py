import os
import subprocess
import time
from pyhive import hive
import csv
import uuid
from read_oplogs import read_oplogs

class Hive:
    table_name = ""
    temp_table = ""

    student_id = ""
    course_id = ""

    current_directory = ""
    primary_keys = None

    def __init__(self, table_name, host, port, username, auth, password, primary_keys=None):
        folder_name = 'mytables'
        current_directory = os.getcwd()
        folder_path = os.path.join(current_directory, folder_name)

        if not os.path.exists(folder_path):
            os.mkdir(folder_path)
        else:
            pass

        self.current_directory = os.getcwd()
        self.primary_keys=primary_keys

        self.table_name = table_name
        self.temp_table = table_name + "_temp"

        hive_home = os.environ.get('HIVE_HOME')
        if not hive_home:
            print("HIVE_HOME is not set.")
            exit(1)

        hive_server_cmd = os.path.join(hive_home, "bin", "hiveserver2")

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
            self.cursor.execute("SET hive.auto.convert.join=true")
            self.cursor.execute("SET hive.exec.max.dynamic.partitions=200")
            self.cursor.execute("SET hive.exec.max.dynamic.partitions.pernode=200")
        except Exception as e:
            print(f"Error connecting to Hive: {e}")
            self.destroy()

    def load_csv(self, filename = 'student_course_grades.csv'):
        try:
            filepath = os.path.join(os.getcwd(), filename)
            query = f"LOAD DATA LOCAL INPATH '{filepath}' INTO TABLE {self.table_name}"
            self.cursor.execute(query)
            print(f"Data loaded from {filepath} into table {self.temp_table}.")
        except Exception as e:
            print(f"Error loading data: {e}")

    def create_table(self, filename = 'student_course_grades.csv'):
        self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
        self.cursor.execute(f"""
            CREATE TABLE {self.table_name} (
                student_id STRING,
                course_id STRING,
                roll_no STRING,
                email_id STRING,
                grade STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            LOCATION 'mytables/student_grades'
            TBLPROPERTIES ("skip.header.line.count"="1")

        """)
        print(f"Table '{self.table_name}' created.")
        self.load_csv()

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
                PARTITION (`student-ID`='{self.student_id}', `course-id`='{self.course_id}')
            """
            self.cursor.execute(load_query)
        
        finally:
            if os.path.exists(temp_filename):
                os.remove(temp_filename)

    def select_data(self, pk=None):
        print(pk)
        if pk is not None:
            self.student_id, self.course_id = pk
        else:
            self.student_id = None
            self.course_id = None

        try:
            if self.student_id and self.course_id:
                query = f"SELECT * FROM {self.table_name} WHERE student_id = '{self.student_id}' AND course_id = '{self.course_id}'"
                self.cursor.execute(query)
            else:
                query = f"SELECT * FROM {self.table_name}"
                self.cursor.execute(query)

            rows = self.cursor.fetchall()
            return rows
        
        except hive.Error as e:
            print(f"Hive error: {e}")
            return None

    def update_data(self, pk, new_grade):
        self.student_id, self.course_id = pk
        self.cursor.execute(f"""
        SELECT student_id, course_id, roll_no, email_id, grade
        FROM {self.table_name}
        """)
        all_rows = self.cursor.fetchall()
        if not all_rows:
            print("Table is empty—nothing to update.")
            return

        tmp_csv = f"/tmp/hive_update_{uuid.uuid4().hex}.csv"
        with open(tmp_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['student_id', 'course_id', 'roll_no', 'email_id', 'grade'])
            for sid, cid, roll, email, grade in all_rows:
                if sid == self.student_id and cid == self.course_id:
                    grade = new_grade
                writer.writerow([sid, cid, roll, email, grade])

        self.cursor.execute(f"""
        LOAD DATA LOCAL INPATH '{tmp_csv}'
        OVERWRITE INTO TABLE {self.table_name}
        """)
        os.remove(tmp_csv)

        print(f"Updated grade for ({self.student_id}, {self.course_id}) → '{new_grade}'.")


    def merge(self, other_system_name: str):
        my_logs = read_oplogs('HIVE')
        other_logs = read_oplogs(other_system_name)
        
        with open('oplogs.hive', 'a') as hive_oplog:
            for pk in self.primary_keys:
                if pk in other_logs:
                   
                    if pk not in my_logs or other_logs[pk][0] > my_logs[pk][0]:
                        latest_ts, latest_value = other_logs[pk]
                       
                        hive_oplog.write(f"{latest_ts}, HIVE.SET(({pk[0]},{pk[1]}), {latest_value})\n")
                        self.update_data(pk, latest_value)
                        print(f"Merged ({pk[0]}, {pk[1]}) from {other_system_name} into Hive at ts={latest_ts}")

        hive_oplog.close()


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
    hive_instance = Hive("student_grades", "localhost", 10000, "vaibhav", "CUSTOM", "Badminton@2468")
    hive_instance.create_table('student_course_grades.csv')
    hive_instance.select_data()
    hive_instance.update_data(('SID1128', 'CSE004'), 'A')
    hive_instance.select_data()
    hive_instance.__del__()