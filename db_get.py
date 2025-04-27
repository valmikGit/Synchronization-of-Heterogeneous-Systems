def db_get(db_name: str, pk: tuple, mongo_handler=None, hive_handler=None, postgre_handler=None, primary_keys=None):
    if primary_keys and pk not in primary_keys:
        print(f"{pk} not in primary keys.")
        return None
    
    if db_name == "POSTGRESQL":
        return postgre_handler.get("student_course_grades", pk)
    elif db_name == "MONGODB":
        return mongo_handler.get("university_db", "grades_of_students", pk)
    elif db_name == "HIVE":
        return hive_handler.select_data(pk)
    else:
        print(f"Unknown database: {db_name}")
        return None
