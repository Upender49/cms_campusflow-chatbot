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
    "teachers": db["teachers"],
    "subjects": db["subjects"],
    "students": db["students"],
    "uploads": db["uploads"]
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
    
    for sec in ['password', 'personal_email', 'phone_number', 'salary', 'paying']:
        if sec in sample: sample[sec] = "[[HIDDEN]]"
    return json.dumps(simplify(sample), indent=2)

schemas = {k: generate_schema_skeleton(v) for k, v in collections.items()}
client_groq = Groq(api_key=GROQ_API_KEY)

# --- 4. PHASE 1: PIPELINE GENERATOR ---
# --- 4. PHASE 1: PIPELINE GENERATOR ---
def text_to_mongo_pipeline(user_question, user_meta=None):
    # Security block removed - rely on Read-Only MongoDB User instead
    # However, still block 'salary' queries at the prompt level if desired, or let projection handle it
    if any(k in user_question.lower() for k in ["salary", "pay"]):
        return None
    system_prompt = f"""
    You are an Expert MongoDB Architect for a College ERP system.
    TASK: Generate a MongoDB Aggregation Pipeline for the 'teachers' collection.
    OUTPUT: Return ONLY a valid JSON array starting with '[' and ending with ']'. NO MARKDOWN. NO ENGLISH. NO EXPLANATIONS.

    ACTIVE USER SESSION: Let the user query info about themselves.
    - Role: {user_meta.get('role') if user_meta else 'unknown'}
    - ID/Username: {user_meta.get('username') if user_meta else 'unknown'}
    - CRITICAL RULE: If the user says "my", "mine", "I", "me", you MUST add a `$match` stage checking their Role ID against the relevant collection document (e.g. `$match: {{"roll_no": "{user_meta.get('username') if user_meta else ''}"}}`).

    DYNAMIC SCHEMAS (For reference):
    teachers: {schemas['teachers']}
    subjects: {schemas['subjects']}
    students: {schemas['students']}

    CRITICAL SCHEMA PATHS & QUIRKS (MUST FOLLOW EXACTLY):
    - Primary Collection: 'teachers'
    - Identifiers: `first_name`, `last_name`, `dept`, `roll_no`, `college_email`
    - subjects Handled: `subjects_handle` (Array of objects)
      - Nested Subject Ref: `subjects_handle.subject.subject_id`
    - Mentees: `mentor_to` (Array of objects)
      - Nested s Array: `mentor_to.s` (Array of ObjectIds referencing s)

    AGGREGATION RULES & BEST PRACTICES:
    1. Base Search:
       - Match `first_name` or `last_name` using `$regex` with `"$options": "i"`.
       - Example: {{"$match": {{"$or": [{{"first_name": {{"$regex": "name", "$options": "i"}}}}, {{"last_name": {{"$regex": "name", "$options": "i"}}}}]}}}}
    2. Joining subjects Taught:
       - You MUST `$unwind: {{ path: "$subjects_handle", preserveNullAndEmptyArrays: true }}`.
       - Then `$lookup` 'subjects' using localField: "subjects_handle.subject.subject_id", foreignField: "_id", as: "subject_details".
       - Unwind `subject_details`.
       - CRITICAL: If filtering by subject name (e.g., "teaching Data Structures"), you MUST `$match` on `subject_details.name` AFTER this lookup stage. DO NOT check strings against `subject_id`.
    3. Joining Mentees (DOUBLE UNWIND REQUIRED):
       - If querying for students a teacher mentors, you MUST unwind sequentially:
       - 1st: `$unwind: {{ path: "$mentor_to", preserveNullAndEmptyArrays: true }}`
       - 2nd: `$unwind: {{ path: "$mentor_to.students", preserveNullAndEmptyArrays: true }}`
       - THEN `$lookup` 'students' using localField: "mentor_to.students", foreignField: "_id", as: "mentee_details".
    4. Clean Output:
       - Use a final `$project` stage. ALWAYS include `first_name`, `last_name`, and `dept`. Include `subject_details.name` or `mentee_details.first_name` depending on the user's question.
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
            
        print(f"\n[DEBUG - TEACHER] --- Phase 1: Raw AI Pipeline Output ---")
        print(raw_content)
        print("-" * 50)
        
        # Try direct JSON parsing first, fallback to extract_json_array
        try:
            return json.loads(raw_content)
        except json.JSONDecodeError:
            return extract_json_array(raw_content)
            
    except Exception as e:
        print(f"[ERROR - TEACHER] Pipeline Gen: {e}")
        return None

# --- 5. PHASE 2: EXECUTE AND EXPLAIN ---
def execute_and_explain(user_question, user_meta=None, stream=False):
    try:
        pipeline = text_to_mongo_pipeline(user_question, user_meta)
        if not pipeline:
            return {{"status": False, "ai_response": "I cannot access that information or the query failed."}}

        db_data = []
        import pymongo.errors
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                raw_results = list(collections['teachers'].aggregate(pipeline))
                db_data = convert_bson_to_json(raw_results)
                break # Success
            except pymongo.errors.OperationFailure as e:
                print(f"[DEBUG - TEACHER] Attempt {attempt+1} Failed: {str(e)}. Retrying...")
                if attempt == max_retries - 1:
                    return {"status": False, "ai_response": "I cannot access that teacher information or the query failed."}
                
                # Ask LLM to fix the pipeline
                fix_prompt = f"The previous pipeline failed with error: {str(e)}. Provide ONLY a corrected JSON array pipeline starting with '[' and ending with ']'."
                user_question = f"{user_question}\n{fix_prompt}"
                pipeline = text_to_mongo_pipeline(user_question, user_meta)
                if not pipeline:
                     return {"status": False, "ai_response": "Failed to generate a corrected pipeline."}

        # Cleanup sensitive data
        RESTRICTED = ['password', 'salary', 'personal_email', 'phone_number']
        for doc in db_data:
            for key in RESTRICTED:
                if key in doc: del doc[key]

        print(f"\n[DEBUG - TEACHER] --- Phase 2: Found {len(db_data)} Records ---")
        print(f"Data: {json.dumps(db_data[:2], indent=2)}")
        print("-" * 50)

        summary_prompt = f"""
        User Question: {user_question}
        Database Results: {json.dumps(db_data[:5])}
        
        CRITICAL: The database FOUND the records above. 
        If results exist, write a friendly summary mentioning EVERY teacher's name found in the database results. Do not just list one if there are multiple.
        If results are empty, say no records were found for that specific query.
        
        ADDITIONAL INSTRUCTION: Use very simple, easy-to-understand English. If the user is asking about their own details (e.g., "my teachers", "I", "me"), address them directly using "You" or "Your" and DO NOT mention their name in the third person.
        """
        
        if not stream:

        
            summary_res = client_groq.chat.completions.create(
                messages=[{"role": "system", "content": "You are a helpful college assistant."}, {"role": "user", "content": summary_prompt}],
                model="llama-3.1-8b-instant"
            )
            ai_summary = summary_res.choices[0].message.content.strip()
    
            print(f"\n[DEBUG - TEACHER] --- Phase 3: AI Final Summary ---")
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

        
                    messages=[{"role": "system", "content": "You are a helpful college assistant."}, {"role": "user", "content": summary_prompt}],

        
                    model="llama-3.1-8b-instant",

        
                    stream=True

        
                )

        
                for chunk in summary_res_stream:

        
                    content = chunk.choices[0].delta.content

        
                    if content:

        
                        yield f"data: {json.dumps({'type': 'chunk', 'text': content})}\n\n"

        
            return generate()
    except Exception as e:
        return {"status": False, "ai_response": f"Teacher Agent Error: {str(e)}"}

if __name__ == "__main__":
    print("----- Teacher AI Agent (Phase Mode) -----")
    while True:
        q = input("\nAsk Teacher Query: ")
        if q.lower() == "exit": break
        print(json.dumps(execute_and_explain(q), indent=2))