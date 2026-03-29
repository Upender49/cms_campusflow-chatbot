import pymongo
import json
from bson import ObjectId

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['clg_erp']

def print_doc(msg, doc):
    print(f"\n--- {msg} ---")
    if doc:
        # Just print keys and types
        info = {k: str(type(v).__name__) for k, v in doc.items()}
        print(f"Keys/Types: {info}")
        # Print actual values for critical fields
        critical = ["_id", "roll_no", "student_id", "subject_id", "accreditation", "address", "first_name", "last_name"]
        content = {k: doc[k] for k in critical if k in doc}
        print(f"Critical Content: {content}")
    else:
        print("Not found.")

print_doc("College Sample", db['college'].find_one())
student = db['students'].find_one({"roll_no": "ME2022015"})
print_doc("Student Sample (ME2022015)", student)
if student:
    attendance = db['attendances'].find_one({"student_id": student['_id']})
    print_doc("Attendance Sample", attendance)
