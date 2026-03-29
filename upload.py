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
    "uploads": db["uploads"],  # Matches lowercase name in your Compass image
    "students": db["students"],
    "teachers": db["teachers"],
    "admins": db["admins"],
    "college": db["college"]
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

# --- 4. PHASE 1: PIPELINE GENERATOR (UPLOADS FOCUS) ---
def text_to_mongo_pipeline(user_question, user_meta=None):
    # Security block removed - rely on Read-Only MongoDB User instead
    # CRITICAL: We use {{ and }} to escape braces for literal MongoDB JSON in f-strings
    system_prompt = f"""
    You are an Expert MongoDB Architect for a College File Management System.
    TASK: Generate a MongoDB Aggregation Pipeline for the 'uploads' collection.
    OUTPUT: Return ONLY a valid JSON array. NO MARKDOWN. NO ENGLISH. NO EXPLANATIONS.

    ACTIVE USER SESSION: Let the user query info about themselves.
    - Role: {user_meta.get('role') if user_meta else 'unknown'}
    - ID/Username: {user_meta.get('username') if user_meta else 'unknown'}
    - CRITICAL RULE: If the user says "my uploads", "files I uploaded", you MUST `$lookup` the relevant user collection based on their Role, and `$match` on their username!

    DYNAMIC SCHEMAS:
    Uploads: {schemas['uploads']}

    CRITICAL SCHEMA PATHS:
    - Root: `file_name`, `file_url`, `file_size`, `original_name`, `format`
    - References: `college` (Ref: college), `user_id` (Polymorphic Ref), `user_type` (Enum: Admin, Student, Teacher)

    AGGREGATION RULES & BEST PRACTICES:
    1. LOWERCASE COLLECTIONS: Always use "students", "teachers", "admins", and "college" (all lowercase) in $lookup.
    2. RESOLVING POLYMORPHIC USERS (CRITICAL):
       - If the user asks "Who uploaded this?", you must perform conditional lookups based on `user_type`.
       - Example Strategy:
         - lookup from: "students", localField: "user_id", foreignField: "_id", as: "student_u"
         - lookup from: "teachers", localField: "user_id", foreignField: "_id", as: "teacher_u"
         - lookup from: "admins", localField: "user_id", foreignField: "_id", as: "admin_u"
    3. FILTERING:
       - Use $regex with $options: "i" for `file_name`, `original_name`, or `format` (e.g., matching ".pdf").
    4. PROJECTION:
       - Return `file_name`, `file_url`, `file_size`, `uploader_type`: "$user_type".
       - If names were resolved, return `uploader_name` by checking which lookup array has data.
    """
    try:
        response = client_groq.chat.completions.create(
            messages=[
                {"role": "system", "content": system_prompt}, 
                {"role": "user", "content": f"Create an upload search query for: {user_question}"}
            ],
            model="llama-3.3-70b-versatile",
            temperature=0
        )
        raw_content = response.choices[0].message.content.strip()
        
        if raw_content.startswith("```"):
            raw_content = re.sub(r"^```(?:json)?|```$", "", raw_content).strip()
            
        print(f"\n[DEBUG - UPLOADS] --- Phase 1: AI Generated Pipeline ---")
        print(raw_content)
        print("-" * 50)
        
        try:
            return json.loads(raw_content)
        except json.JSONDecodeError:
            return extract_json_array(raw_content)
            
    except Exception as e:
        print(f"[ERROR - UPLOADS] Pipeline Gen: {e}")
        return None

# --- 5. PHASE 2: EXECUTE AND EXPLAIN ---
def execute_and_explain(user_question, user_meta=None, stream=False):
    try:
        pipeline = text_to_mongo_pipeline(user_question, user_meta)
        if not pipeline:
            return {"status": False, "ai_response": "I cannot access the file records right now."}

        db_data = []
        import pymongo.errors
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                raw_results = list(collections['uploads'].aggregate(pipeline))
                db_data = convert_bson_to_json(raw_results)
                break # Success
            except pymongo.errors.OperationFailure as e:
                print(f"[DEBUG - UPLOADS] Attempt {attempt+1} Failed: {str(e)}. Retrying...")
                if attempt == max_retries - 1:
                    return {"status": False, "ai_response": "I cannot access that upload information or the query failed."}
                
                # Ask LLM to fix the pipeline
                fix_prompt = f"The previous pipeline failed with error: {str(e)}. Provide ONLY a corrected JSON array pipeline starting with '[' and ending with ']'."
                user_question = f"{user_question}\n{fix_prompt}"
                pipeline = text_to_mongo_pipeline(user_question, user_meta)
                if not pipeline:
                     return {"status": False, "ai_response": "Failed to generate a corrected pipeline."}

        print(f"\n[DEBUG - UPLOADS] --- Phase 2: Found {len(db_data)} Matching Files ---")
        print(f"Data: {json.dumps(db_data[:3], indent=2)}")
        print("-" * 50)

        summary_prompt = f"""
        User Question: {user_question}
        Database Results: {json.dumps(db_data[:10])}
        
        Task: Write a helpful 2-sentence summary of the files found. 
        Mention the file name, format, and size. If uploader info exists, mention that too.
        If empty [], say no matching files were found.
        
        ADDITIONAL INSTRUCTION: Use very simple, easy-to-understand English. If the user is asking about their own files or data, address them directly using "You" or "Your" and DO NOT mention their name in the third person.
        """
        
        if not stream:

        
            summary_res = client_groq.chat.completions.create(
                messages=[{"role": "system", "content": "You are a helpful college document assistant."}, {"role": "user", "content": summary_prompt}],
                model="llama-3.1-8b-instant"
            )
            ai_summary = summary_res.choices[0].message.content.strip()
    
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

        
                    messages=[{"role": "system", "content": "You are a helpful college document assistant."}, {"role": "user", "content": summary_prompt}],

        
                    model="llama-3.1-8b-instant",

        
                    stream=True

        
                )

        
                for chunk in summary_res_stream:

        
                    content = chunk.choices[0].delta.content

        
                    if content:

        
                        yield f"data: {json.dumps({'type': 'chunk', 'text': content})}\n\n"

        
            return generate()
    except Exception as e:
        return {"status": False, "ai_response": f"Upload Agent Error: {str(e)}"}

if __name__ == "__main__":
    print("----- Uploads AI Agent (Phase Mode) -----")
    while True:
        q = input("\nSearch for a file: ")
        if q.lower() == "exit": break
        print(json.dumps(execute_and_explain(q), indent=2))