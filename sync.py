import csv
from collections import defaultdict
import json
from MongoDB_connect import MongoDBHandler
from postgresql_connector import PostgreSQLHandler

# Initialize 2D dictionaries using defaultdict
mongo_logs = defaultdict(lambda: defaultdict(str))
hive_logs = defaultdict(lambda: defaultdict(str))
postgresql_logs = defaultdict(lambda: defaultdict(str))

# Path to your CSV file
csv_file_path= 'student_course_grades.csv'  # Replace with your actual path

primary_keys = []

# Read the CSV and initialize logs
with open(csv_file_path, mode='r', newline='', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    
    for row in reader:
        student_id = row['student-ID'].strip()
        course_id = row['course-id'].strip()
        key = (student_id, course_id)

        primary_keys.append(key)

        # Initialize with timestamp 0 and value ""
        mongo_logs[key] = (0, "")
        hive_logs[key] = (0, "")
        postgresql_logs[key] = (0, "")

db_logs_map = {
    'HIVE': hive_logs,
    'MONGODB': mongo_logs,
    'POSTGRESQL': postgresql_logs
}

# Example db_logs_map dictionary for fallback in-memory loging


mongo_handler=MongoDBHandler()

def db_set(db_name: str, pk: tuple, value: str, ts: int):
    if db_name == "MONGODB":
        mongo_handler.set("university_db", "grades_of_students", pk, value, ts)
        db_logs_map[db_name][pk] = (ts, value)
    else:
        db_logs_map[db_name][pk] = (ts, value)



# def db_set(db_name:str, pk:tuple, value:str, ts:int):
#     Mongodb_connect()
#     db_logs_map[db_name][pk] = (ts, value)
    # Add appropriate connector

def merge(db1:str, db2:str, ts:int)->list:
    db1_logs = db_logs_map[db1]
    db2_logs = db_logs_map[db2]

    for pk in primary_keys:
        if(db2_logs[pk][0] > db1_logs[pk][0]):
            db_set(db_name=db1, pk=pk, value=db2_logs[pk][1], ts=ts)

def db_get(db_name:str, pk:tuple):
    """
    GET the grade for the given primary key tuple.
    """
    pass

def parse_json_instructions(file_path:str)->list:
    # Open and load the JSON data
    data = None
    with open(file_path, 'r') as file:
        data = json.load(file)

    # Print the loaded data
    return data

instructions = parse_json_instructions("parsed_instructions.json")

for inst in instructions:
    if inst["operation"] == 'GET':
        if inst["db1"] == 'MONGODB':
            # Add mongo connector
            pass
        elif inst["db1"] == 'POSTGRESQL':
            # Add postgresql connector
            pass
        elif inst["db1"] == 'HIVE':
            # Add hive connector
            pass

    elif inst["operation"] == 'SET':
        if inst["db1"] == 'MONGODB':
            db_set(db_name=inst["db1"], pk=(inst["student_id"], inst["course_id"]), value=inst["grade"], ts=inst["timestamp"])
            # Add mongo connector

        elif inst["db1"] == 'POSTGRESQL':
            db_set(db_name=inst["db1"], pk=(inst["student_id"], inst["course_id"]), value=inst["grade"], ts=inst["timestamp"])
            # Add postgresql connector

        elif inst["db1"] == 'HIVE':
            db_set(db_name=inst["db1"], pk=(inst["student_id"], inst["course_id"]), value=inst["grade"], ts=inst["timestamp"])
            # Add hive connector

    elif inst["operation"] == 'MERGE':
        if inst["db1"] == 'MONGODB' and inst["db2"] == 'POSTGRESQL':
            merge(db1=inst["db1"], db2=inst["db2"], ts=inst["timestamp"])
        
        elif inst["db1"] == 'POSTGRESQL' and inst["db2"] == 'MONGODB':
            merge(db1=inst["db1"], db2=inst["db2"], ts=inst["timestamp"])
        
        elif inst["db1"] == 'POSTGRESQL' and inst["db2"] == 'HIVE':
            merge(db1=inst["db1"], db2=inst["db2"], ts=inst["timestamp"])
        
        elif inst["db1"] == 'HIVE' and inst["db2"] == 'POSTGRESQL':
            merge(db1=inst["db1"], db2=inst["db2"], ts=inst["timestamp"])
        
        elif inst["db1"] == 'MONGODB' and inst["db2"] == 'HIVE':
            merge(db1=inst["db1"], db2=inst["db2"], ts=inst["timestamp"])

        elif inst["db1"] == 'HIVE' and inst["db2"] == 'MONGODB':
            merge(db1=inst["db1"], db2=inst["db2"], ts=inst["timestamp"])