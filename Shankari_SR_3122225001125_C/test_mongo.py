from pymongo import MongoClient

uri = "mongodb://localhost:27017/"  # Change if your MongoDB is remote
client = MongoClient(uri)

try:
    result = client.admin.command("ping")
    print("MongoDB connection successful:", result)
except Exception as e:
    print("MongoDB connection failed:", e)
finally:
    client.close()
