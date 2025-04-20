import csv
from collections import defaultdict





def MongoDB_connect():
    uri = "mongodb+srv://mittalvaibhav277:GLP5SHfCbxQdiWPm@cluster0.xghzn4m.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

# Initialize 2D dictionaries using defaultdict
mongo_logs = defaultdict(lambda: defaultdict(str))
hive_logs = defaultdict(lambda: defaultdict(str))
postgresql_logs = defaultdict(lambda: defaultdict(str))

# Path to your CSV file
csv_file_path = 'student_course_grades.csv'  # Replace with your actual path

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
    "HIVE": hive_logs,
    "MONGODB": mongo_logs,
    "POSTGRESQL": postgresql_logs
}
MongoDB_connect()
print("Printing mongo logs")
print(mongo_logs)
print("Printing hive logs")
print(hive_logs)
print("Printing postgresql logs")
print(postgresql_logs)
print("Printing primary keys")
print(primary_keys)

def db_set(db_name:str, pk:tuple, value:str, ts:int):
    db_logs_map[db_name][pk] = (ts, value)

def merge(db1:str, db2:str, ts:int):
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