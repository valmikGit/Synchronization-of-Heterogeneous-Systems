


from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


class MongoDBHandler:
    def __init__(self, uri: str = "mongodb+srv://mittalvaibhav277:GLP5SHfCbxQdiWPm@cluster0.xghzn4m.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"):
        self.uri = uri
        self.client = None
        self.db = None
        self.connect()

    def connect(self):
        try:
            self.client = MongoClient(self.uri, server_api=ServerApi('1'))
            self.client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print("Connection failed:", e)

    def set(self, db_name: str, collection_name: str, pk: tuple, value: str, ts: int):
        db = self.client[db_name]
        collection = db[collection_name]
        key = {"pk": pk}
        update = {"$set": {"value": value, "ts": ts}}
        collection.update_one(key, update, upsert=True)

    def get(self, db_name: str, collection_name: str, pk: tuple):
        db = self.client[db_name]
        collection = db[collection_name]
        return collection.find_one({"pk": pk})

