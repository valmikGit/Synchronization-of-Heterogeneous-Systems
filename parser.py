import re
import json

def parse_testcase_file(file_path):
    instructions = []

    mongo_logger = open('oplogs.mongo', 'w')
    hive_logger = open('oplogs.hive', 'w')
    postgresql_logger = open('oplogs.postgresql', 'w')

    with open(file_path, 'r') as file:
        for idx, line in enumerate(file):
            line = line.strip()
            if not line:
                continue  # Skip empty lines

            # Row number becomes the timestamp
            timestamp = idx + 1
            db_timestamp = None
            operation = None
            db1 = db2 = student_id = course_id = grade = None

            # Check for leading timestamp (db_timestamp)
            if ',' in line:
                parts = line.split(',', 1)
                if parts[0].strip().isdigit():
                    db_timestamp = int(parts[0].strip())
                    line = parts[1].strip()

            # Parse MERGE
            if 'MERGE' in line:
                match = re.match(r'(\w+)\.MERGE\((\w+)\)', line)
                if match:
                    db1, db2 = match.groups()
                    operation = 'MERGE'

            # Parse SET
            elif 'SET' in line:
                match = re.match(r'(\w+)\.SET\(\(([^,]+),([^)]+)\),\s*([^)]+)\)', line)
                if match:
                    db1, student_id, course_id, grade = match.groups()
                    operation = 'SET'

            # Parse GET
            elif 'GET' in line:
                match = re.match(r'(\w+)\.GET\(([^,]+),([^)]+)\)', line)
                if match:
                    db1, student_id, course_id = match.groups()
                    operation = 'GET'

            # Append parsed instruction as dictionary
            instructions.append({
                'timestamp': timestamp,         # Line number
                'db_timestamp': db_timestamp,   # Optional prefix timestamp
                'operation': operation,
                'db1': db1,
                'db2': db2,
                'student_id': student_id,
                'course_id': course_id,
                'grade': grade
            })

            if db1 == "MONGODB":
                if operation == "SET":
                    mongo_logger.write(f"{db_timestamp}, {db1}.SET(({student_id},{course_id}), {grade})\n")
                elif operation == 'GET':
                    mongo_logger.write(f"{db_timestamp}, {db1}.GET({student_id},{course_id})\n")
            
            elif db1 == "HIVE":
                if operation == "SET":
                    hive_logger.write(f"{db_timestamp}, {db1}.SET(({student_id},{course_id}), {grade})\n")
                elif operation == 'GET':
                    hive_logger.write(f"{db_timestamp}, {db1}.GET({student_id},{course_id})\n")

            elif db1 == "POSTGRESQL":
                if operation == "SET":
                    postgresql_logger.write(f"{db_timestamp}, {db1}.SET(({student_id},{course_id}), {grade})\n")
                elif operation == 'GET':
                    postgresql_logger.write(f"{db_timestamp}, {db1}.GET({student_id},{course_id})\n")

    mongo_logger.close()
    postgresql_logger.close()
    hive_logger.close()

    return instructions

# Parse instructions
parsed_instructions = parse_testcase_file("example_testcase.in")

# Save to JSON file
with open("parsed_instructions.json", "w") as json_file:
    json.dump(parsed_instructions, json_file, indent=4)

# Optional: print to confirm
print("Instructions saved to parsed_instructions.json")