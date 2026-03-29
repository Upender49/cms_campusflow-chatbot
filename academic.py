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
    "academics": db["academics"],
    "students": db["students"], # Ensure this matches your actual students collection name
    "subjects": db["subjects"],
    "teachers": db["teachers"]
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

# --- 4. PHASE 1: PIPELINE GENERATOR (ACADEMIC FOCUS) ---
# --- 4. PHASE 1: PIPELINE GENERATOR (ACADEMIC FOCUS) ---
def text_to_mongo_pipeline(user_question, user_meta=None):
    # Security block removed - rely on Read-Only MongoDB User instead
    username = user_meta.get('username') if user_meta else 'unknown'
    role     = user_meta.get('role')     if user_meta else 'unknown'

    system_prompt = f"""
    You are an Expert MongoDB Architect for a College ERP system.
    TASK: Generate a MongoDB Aggregation Pipeline for the 'academics' collection.
    OUTPUT: Return ONLY a valid JSON array. NO MARKDOWN. NO ENGLISH. NO EXPLANATIONS.

    ACTIVE USER SESSION:
    - Role: {role}
    - Username (roll_no): {username}

    =====================================================================
    !! CRITICAL SCHEMA RULE — READ THIS FIRST, NEVER VIOLATE IT !!
    =====================================================================
    The 'academics' collection has a field called `student_id` which stores
    a MongoDB ObjectId that references the `_id` field of the 'students'
    collection.  The username / roll number (e.g. "ME2022015") is stored
    in the 'students' collection under the field `roll_no`.

    THEREFORE:
      ✅ CORRECT  — look up the students collection, THEN match on roll_no:
        {{"$lookup": {{"from": "students", "localField": "student_id",
                      "foreignField": "_id", "as": "student_info"}}}},
        {{"$unwind": {{"path": "$student_info", "preserveNullAndEmptyArrays": true}}}},
        {{"$match": {{"student_info.roll_no": "{username}"}}}}

      ❌ FORBIDDEN — NEVER do this (student_id is an ObjectId, not a string):
        {{"$match": {{"student_id": "{username}"}}}}   ← WRONG, returns 0 results

    When the user says "my subjects / my marks / my attendance / my teachers",
    ALWAYS start the pipeline with the correct $lookup → $unwind → $match
    pattern shown above before doing anything else.
    =====================================================================

    DYNAMIC SCHEMAS (For reference):
    academics : {schemas['academics']}
    students  : {schemas['students']}
    subjects  : {schemas['subjects']}
    teachers  : {schemas['teachers']}

    CRITICAL SCHEMA PATHS FOR 'academics':
    - Student Ref : `student_id`  (ObjectId → students._id)
    - Years Array : `years`
      - Mentor Ref   : `years.mentor`
      - Semesters    : `years.semesters`  (OBJECT, not an array — do NOT $unwind it)
        - Subjects   : `years.semesters.registerd_subjects`  (Array)
          - subject_id : ObjectId → subjects._id
          - teacher    : ObjectId → teachers._id
          - attendance : embedded object

    AGGREGATION RULES:
    1. Pipeline runs on the 'academics' collection.
    2. To filter by the active user, ALWAYS do:
         $lookup students → $unwind student_info → $match student_info.roll_no == "{username}"
    3. To get subject details, $unwind "$years" then $unwind "$years.semesters.registerd_subjects",
       then $lookup subjects on localField "years.semesters.registerd_subjects.subject_id".
    4. To get teacher details, $lookup teachers on localField "years.semesters.registerd_subjects.teacher".
    5. To get mentor details, $unwind "$years" then $lookup teachers on localField "years.mentor".
    6. Always end with a clean $project showing only the fields the user asked for.
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
            
        print(f"\n[DEBUG - ACADEMIC] --- Phase 1: AI Generated Pipeline ---")
        print(raw_content)
        print("-" * 50)
        
        # Try direct JSON parsing first, fallback to extract_json_array
        try:
            return json.loads(raw_content)
        except json.JSONDecodeError:
            return extract_json_array(raw_content)
            
    except Exception as e:
        print(f"[ERROR - ACADEMIC] Pipeline Gen: {e}")
        return None

# --- 5. PHASE 2: EXECUTE AND EXPLAIN ---
def execute_and_explain(user_question, user_meta=None, stream=False):
    try:
        pipeline = text_to_mongo_pipeline(user_question, user_meta)
        if not pipeline:
            return {"status": False, "ai_response": "I cannot access that academic information or the query failed."}

        db_data = []
        import pymongo.errors
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                raw_results = list(collections['academics'].aggregate(pipeline))
                db_data = convert_bson_to_json(raw_results)
                break # Success
            except pymongo.errors.OperationFailure as e:
                print(f"[DEBUG - ACADEMIC] Attempt {attempt+1} Failed: {str(e)}. Retrying...")
                if attempt == max_retries - 1:
                    return {"status": False, "ai_response": "I cannot access that academic information or the query failed."}
                
                # Ask LLM to fix the pipeline
                fix_prompt = f"The previous pipeline failed with error: {str(e)}. Provide ONLY a corrected JSON array pipeline starting with '[' and ending with ']'."
                user_question = f"{user_question}\n{fix_prompt}"
                pipeline = text_to_mongo_pipeline(user_question, user_meta)
                if not pipeline:
                     return {"status": False, "ai_response": "Failed to generate a corrected pipeline."}

        print(f"\n[DEBUG - ACADEMIC] --- Phase 2: Found {len(db_data)} Records ---")
        print(f"Data: {json.dumps(db_data[:2], indent=2)}")
        print("-" * 50)

        summary_prompt = f"""
        User Question: {user_question}
        Database Results: {json.dumps(db_data[:5])}
        
        CRITICAL: The database FOUND the person/data above. 
        If results exist, write a friendly 2-sentence summary based ONLY on the provided JSON data.
        If results are empty, politely state that no academic records match that description.

        ADDITIONAL INSTRUCTION: Use very simple, easy-to-understand English. If the user is asking about their own details (e.g., "my teachers", "I", "me"), address them directly using "You" or "Your" and DO NOT mention their name (like "John Doe") in the third person.
        """
        
        if not stream:

        
            summary_res = client_groq.chat.completions.create(
                messages=[{"role": "system", "content": "You are a helpful academic advisor assistant."}, {"role": "user", "content": summary_prompt}],
                model="llama-3.1-8b-instant"
            )
            ai_summary = summary_res.choices[0].message.content.strip()
    
            print(f"\n[DEBUG - ACADEMIC] --- Phase 3: AI Final Summary ---")
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

        
                    messages=[{"role": "system", "content": "You are a helpful academic advisor assistant."}, {"role": "user", "content": summary_prompt}],

        
                    model="llama-3.1-8b-instant",

        
                    stream=True

        
                )

        
                for chunk in summary_res_stream:

        
                    content = chunk.choices[0].delta.content

        
                    if content:

        
                        yield f"data: {json.dumps({'type': 'chunk', 'text': content})}\n\n"

        
            return generate()
    except Exception as e:
        return {"status": False, "ai_response": f"Academic Agent Error: {str(e)}"}

if __name__ == "__main__":
    print("----- Academic AI Agent (Phase Mode) -----")
    while True:
        q = input("\nAsk Academic Query: ")
        if q.lower() == "exit": break
        print(json.dumps(execute_and_explain(q), indent=2))