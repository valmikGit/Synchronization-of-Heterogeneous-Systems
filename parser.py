import re

def parse_testcase_file(file_path):
    instructions = []

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

    return instructions

# Example usage
parsed_instructions = parse_testcase_file("example_testcase.in")

for instr in parsed_instructions:
    print(instr)