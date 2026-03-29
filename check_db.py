import os
from dotenv import load_dotenv
import pymongo

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "cms_campusflow")

client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
students = db['students'].find({})
for s in students:
    if "Rohan" in s.get("first_name", "") or "Iyer" in s.get("last_name", ""):
        print(f"Found: {s.get('first_name')} {s.get('last_name')}, Roll No: {s.get('roll_no')}")
