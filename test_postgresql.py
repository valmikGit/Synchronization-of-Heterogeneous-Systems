from postgresql_connector import PostgreSQLHandler

postgre_handler=PostgreSQLHandler()

def db_set(db_name: str, pk: tuple, value: str, ts: int):
    if db_name == "POSTGRESQL":
        postgre_handler.set("student_course_grades", pk, value, ts)
db_set(db_name="POSTGRESQL", pk=("SID1310", "CSE020"), value="B", ts=1)