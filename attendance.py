import os
import json
import re
from pymongo import MongoClient
from groq import Groq
from dotenv import load_dotenv
from bson import ObjectId
from datetime import datetime

# --- LOAD ENV VARIABLES ---
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "cms_campusflow")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# --- 1. SETUP MONGODB CONNECTION ---
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

collections = {
    "attendances": db["attendances"],
    "students": db["students"], # Ensure this matches your collection name
    "subjects": db["subjects"]
}

# --- 2. HELPER FUNCTIONS ---
def convert_bson_to_json(doc):
    if isinstance(doc, dict): return {k: convert_bson_to_json(v) for k, v in doc.items()}
    elif isinstance(doc, list): return [convert_bson_to_json(i) for i in doc]
    elif isinstance(doc, ObjectId): return str(doc)
    elif isinstance(doc, datetime): return doc.isoformat()
    else: return doc

def extract_json_array(text):
    start_idx = text.find('[')
    end_idx = text.rfind(']')
    if start_idx != -1 and end_idx != -1:
        json_str = text[start_idx : end_idx + 1]
        try:
            return json.loads(json_str)
        except:
            return None
    return None

# --- 3. DYNAMIC SCHEMA GENERATOR ---
def generate_schema_skeleton(collection):
    sample = collection.find_one()
    if not sample: return "Empty"
    def simplify(data):
        if isinstance(data, dict): return {k: simplify(v) for k, v in data.items()}
        elif isinstance(data, list): return [simplify(data[0])] if len(data) > 0 else []
        elif isinstance(data, ObjectId): return "ObjectId"
        else: return str(type(data).__name__)
    return json.dumps(simplify(sample), indent=2)

schemas = {k: generate_schema_skeleton(v) for k, v in collections.items()}
client_groq = Groq(api_key=GROQ_API_KEY)

# --- 4. PHASE 1: PIPELINE GENERATOR (ATTENDANCE FOCUS) ---
# --- 4. PHASE 1: PIPELINE GENERATOR (ATTENDANCE FOCUS) ---
def text_to_mongo_pipeline(user_question, user_meta=None):
    # Security block removed - rely on Read-Only MongoDB User instead
    system_prompt = f"""
    You are an Expert MongoDB Architect for a College Attendance System.
    TASK: Generate a MongoDB Aggregation Pipeline for the 'attendances' collection.
    OUTPUT: Return ONLY a valid JSON array starting with '[' and ending with ']'. NO MARKDOWN. NO ENGLISH.
    
    ACTIVE USER SESSION: Let the user query info about themselves.
    - Role: {user_meta.get('role') if user_meta else 'unknown'}
    - ID/Username: {user_meta.get('username') if user_meta else 'unknown'}
    - CRITICAL RULE: If the user says "my", "mine", "I", "me", you MUST add a `$match` stage on `student_info.roll_no` using their active username ({user_meta.get('username') if user_meta else 'unknown'}).
    - NOTE: You must perform the `$lookup` for 'students' BEFORE you can match on `student_info.roll_no`.

    DYNAMIC SCHEMAS (For reference):
    attendances: {schemas['attendances']}
    students: {schemas['students']}
    subjects: {schemas['subjects']}

    CRITICAL SCHEMA PATHS:
    - Primary Collection: 'attendances'
    - Student Reference: `student_id`
    - Subject Reference: `subject_id`
    - Attendance Records: `day_by_day` (Array of objects containing `date` and `status`)
      - Status enum: 'present', 'absent'

    AGGREGATION RULES:
    1. Lookups: 
       - Always lookup 'students' using localField: "student_id", as: "student_info".
       - Always lookup 'subjects' using localField: "subject_id", as: "subject_info".
       - Use `$unwind: {{ path: "$student_info", preserveNullAndEmptyArrays: true }}` after lookups.
       - Use `$unwind: {{ path: "$subject_info", preserveNullAndEmptyArrays: true }}` after lookups.
    2. Filtering by Student Name/Roll No:
       - Match on `student_info.first_name`, `student_info.last_name`, or `student_info.roll_no`.
    3. Filtering by Subject:
       - Match on `subject_info.name` using `$regex` with `"$options": "i"`.
    4. Calculating Attendance (Total Presents/Absents):
       - If the user asks for total classes attended or missed, you MUST `$unwind: "$day_by_day"`.
       - After unwinding, `$match` on `day_by_day.status` ("present" or "absent").
       - Use `$group` by student and subject to `$sum` the count.
    5. Checking Specific Dates:
       - If the user asks for attendance on a specific date, `$unwind: "$day_by_day"`, then `$match` on `day_by_day.date`.
    6. Advanced Analytics & Averages:
       - If asked for "average attendance", use `$group` to calculate `$avg` or percentages.
    7. Clean Output:
       - Use `$project` to return clean fields: `student_name`, `subject_name`, `date` (if unwound), `status` (if unwound), `average_attendance` or `total_count` (if grouped).
    """
    try:
        response = client_groq.chat.completions.create(
            messages=[
                {"role": "system", "content": system_prompt}, 
                {"role": "user", "content": f"Create an aggregation pipeline for this request: {user_question}"}
            ],
            model="llama-3.3-70b-versatile",
            temperature=0
        )
        raw_content = response.choices[0].message.content.strip()
        
        # Strip markdown code blocks if the LLM accidentally includes them
        if raw_content.startswith("```"):
            raw_content = re.sub(r"^```(?:json)?|```$", "", raw_content).strip()
            
        print(f"\n[DEBUG - ATTENDANCE] --- Phase 1: Raw AI Pipeline Output ---")
        print(raw_content)
        print("-" * 50)
        
        # Try direct JSON parsing first, fallback to extract_json_array
        try:
            return json.loads(raw_content)
        except json.JSONDecodeError:
            return extract_json_array(raw_content)
            
    except Exception as e:
        print(f"[ERROR - ATTENDANCE] Pipeline Gen: {e}")
        return None

# --- 5. PHASE 2: EXECUTE AND EXPLAIN ---
def execute_and_explain(user_question, user_meta=None, stream=False):
    try:
        pipeline = text_to_mongo_pipeline(user_question, user_meta)
        if not pipeline:
            return {"status": False, "ai_response": "I cannot process that attendance query."}

        db_data = []
        import pymongo.errors
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                raw_results = list(collections['attendances'].aggregate(pipeline))
                db_data = convert_bson_to_json(raw_results)
                break # Success
            except pymongo.errors.OperationFailure as e:
                print(f"[DEBUG - ATTENDANCE] Attempt {attempt+1} Failed: {str(e)}. Retrying...")
                if attempt == max_retries - 1:
                    return {"status": False, "ai_response": "I cannot access that attendance information or the query failed."}
                
                # Ask LLM to fix the pipeline
                fix_prompt = f"The previous pipeline failed with error: {str(e)}. Provide ONLY a corrected JSON array pipeline starting with '[' and ending with ']'."
                user_question = f"{user_question}\n{fix_prompt}"
                pipeline = text_to_mongo_pipeline(user_question, user_meta)
                if not pipeline:
                     return {"status": False, "ai_response": "Failed to generate a corrected pipeline."}
        print(f"\n[DEBUG - ATTENDANCE] --- Phase 2: Found {len(db_data)} Records ---")
        print(f"Data: {json.dumps(db_data[:2], indent=2)}")
        print("-" * 50)

        summary_prompt = f"""
        User Question: {user_question}
        Database Results: {json.dumps(db_data[:5])}
        
        TASK:
        1. If "Database Results" is NOT empty:
           - Summarize the attendance details found in the data (subject, count/status).
           - Use "You" and "Your" to address the user directly.
        2. If "Database Results" is empty []:
           - Politely say that no attendance records were found.
        
        STRICT RULES:
        - Max 2 sentences.
        - Friendly and professional.
        - DO NOT mention technical terms like JSON or database.
        - Use simple English.
        """
        
        if not stream:

        
            summary_res = client_groq.chat.completions.create(
                messages=[{"role": "system", "content": "You are a helpful attendance tracking assistant."}, {"role": "user", "content": summary_prompt}],
                model="llama-3.1-8b-instant"
            )
            ai_summary = summary_res.choices[0].message.content.strip()
    
            print(f"\n[DEBUG - ATTENDANCE] --- Phase 3: AI Final Summary ---")
            print(ai_summary)
            print("-" * 50)
    
            return {
                "user_question": user_question,
                "ai_response": ai_summary,
                "database_results": db_data,
                "database_query": pipeline,
                "status": True
            }

        
        else:

        
            def generate():

        
                import json

        
                # Yield metadata first (db_data, pipeline, status might exist, we yield what we know)

        
                metadata = {"type": "metadata", "status": True}

        
                if 'db_data' in locals() or 'db_data' in globals(): metadata['database_results'] = db_data

        
                if 'pipeline' in locals() or 'pipeline' in globals(): metadata['database_query'] = pipeline

        
                yield f"data: {json.dumps(metadata)}\n\n"

        
                

        
                summary_res_stream = client_groq.chat.completions.create(

        
                    messages=[{"role": "system", "content": "You are a helpful attendance tracking assistant."}, {"role": "user", "content": summary_prompt}],

        
                    model="llama-3.1-8b-instant",

        
                    stream=True

        
                )

        
                for chunk in summary_res_stream:

        
                    content = chunk.choices[0].delta.content

        
                    if content:

        
                        yield f"data: {json.dumps({'type': 'chunk', 'text': content})}\n\n"

        
            return generate()
    except Exception as e:
        return {"status": False, "ai_response": f"Attendance Agent Error: {str(e)}"}

if __name__ == "__main__":
    print("----- Attendance AI Agent (Phase Mode) -----")
    while True:
        q = input("\nAsk Attendance Query: ")
        if q.lower() == "exit": break
        print(json.dumps(execute_and_explain(q), indent=2))