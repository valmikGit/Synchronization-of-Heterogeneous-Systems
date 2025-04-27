from postgresql_connector import PostgreSQLHandler

postgre_handler=PostgreSQLHandler()

def db_set(db_name: str, pk: tuple, value: str, ts: int):
    if db_name == "POSTGRESQL":
        postgre_handler.set("student_course_grades", pk, value, ts)
def db_get(db_name:str, pk:tuple)->str:
    if db_name == "POSTGRESQL":
        return postgre_handler.get("student_course_grades", pk)
db_set(db_name="POSTGRESQL", pk=("SID1310", "CSE020"), value="A", ts=1)
db_get(db_name="POSTGRESQL", pk=("SID1310", "CSE020"))