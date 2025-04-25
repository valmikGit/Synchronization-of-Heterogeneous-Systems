from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import csv



class MongoDBHandler:
    def __init__(self, uri: str = "mongodb+srv://mittalvaibhav277:GLP5SHfCbxQdiWPm@cluster0.xghzn4m.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"):
        self.uri = uri
        self.client = None
        self.collection = None
        self.db = None
        self.connect()

    def connect(self):
        try:
            self.client = MongoClient(self.uri, server_api=ServerApi('1'))
            self.client.admin.command('ping')
            self.db = self.client["university_db"]
            self.collection=self.db["grades_of_students"]
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print("Connection failed:", e)

    def set(self, db_name: str, collection_name: str, pk: tuple, value: str, ts: int):
        student_id, course_id = pk
        
        # Build the query and update documents
        query = {
            "student-ID": student_id,
            "course-id": course_id
        }

        update = {
            "$set": {
                "grade": value
            }
        }

        result = self.collection.update_one(query, update)

        if result.matched_count == 0:
            print("No matching document found to update.")
        elif result.modified_count == 1:
            print("Document updated successfully.")
        else:
            print("Document matched but no change was made.")
    def get(self, db_name: str, collection_name: str, pk: tuple):
        db = self.client[db_name]
        collection = db[collection_name]

        # Construct the query based on the composite primary key (assuming it's (student_id, course_id))
        query = {"student-ID": pk[0], "course-id": pk[1]}
        # Perform the find operation
        return collection.find_one(query)

    def insert_student_grade(self, db_name: str, collection_name: str, student_data: dict):
        db = self.client[db_name]
        collection = db[collection_name]
        result = collection.insert_one(student_data)
        print(f"Inserted student record with ID: {result.inserted_id}")

    def bulk_insert_students_from_csv(self, db_name: str, collection_name: str, csv_path: str):
        student_records = []
        with open(csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                student_records.append(row)
        if student_records:
            db = self.client[db_name]
            collection = db[collection_name]
            result = collection.insert_many(student_records)
            print(f"Successfully inserted {len(result.inserted_ids)} student records.")

##