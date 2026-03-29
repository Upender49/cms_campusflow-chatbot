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
    "events": db["events"], # Ensure this matches your collection name
    "students": db["students"],
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

# --- 4. PHASE 1: PIPELINE GENERATOR (events FOCUS) ---
def text_to_mongo_pipeline(user_question, user_meta=None):
    # Security block removed - rely on Read-Only MongoDB User instead
    system_prompt = f"""
    You are an Expert MongoDB Architect for a college Event Management System.
    TASK: Generate a MongoDB Aggregation Pipeline for the 'events' collection.
    OUTPUT: Return ONLY a valid JSON array starting with '[' and ending with ']'. NO MARKDOWN. NO ENGLISH. NO EXPLANATIONS.

    ACTIVE USER SESSION: Let the user query info about themselves.
    - Role: {user_meta.get('role') if user_meta else 'unknown'}
    - ID/Username: {user_meta.get('username') if user_meta else 'unknown'}
    - CRITICAL RULE: If the user says "events I registered for", "my events", "events I am attending", you MUST unwind `registered_memebers`, `$lookup` students on `student_id`, and `$match` that student's `roll_no` to their active Username ID!

    DYNAMIC SCHEMAS (For reference):
    events: {schemas['events']}
    students: {schemas['students']}
    college: {schemas['college']}

    CRITICAL SCHEMA PATHS FOR 'events' (MUST FOLLOW THESE EXACT PATHS):
    - Name & Details: `name`, `desc`, `organized_by`
    - Dates: `from_date` and `to_date` (These are ISODate objects).
    - Times: `from_time` and `to_time` (These are Strings).
    - Registered Attendees: `registered_memebers` (CRITICAL: Note the spelling is exactly 'registered_memebers' with an extra 'e').
      - Student Ref: `registered_memebers.student_id`
    - Contacts Array: `contacts` (Array of objects containing `name`, `mobile`, `email`)
    - Creator Info: `created_by` (ObjectId) and `created_type` (String: 'Admin', 'Student', 'Teacher')

    AGGREGATION RULES & BEST PRACTICES:
    1. Primary Collection: The pipeline runs directly on the 'events' collection.
    2. Event Name Matching:
       - Use `$regex` with `"$options": "i"` for searching event names or descriptions.
    3. Counting Attendees (Super Important):
       - If the user asks "How many students registered" or "attendee count", do NOT unwind. 
       - Instead, use `$project` with `$size: {{ "$ifNull": [ "$registered_memebers", [] ] }}` to safely count the array length.
    4. Finding Specific Attendees (Lookup):
       - If the user asks "Who is attending" or for student names, you MUST `$unwind: {{ path: "$registered_memebers", preserveNullAndEmptyArrays: false }}`.
       - Then use `$lookup` on 'students' with localField: "registered_memebers.student_id".
       - Unwind the resulting student info to project `first_name` and `last_name`.
    5. Contact Information:
       - If the user asks who to contact for an event, include the `contacts` array in your `$project` stage. Do not unwind it unless filtering by a specific contact.
    6. Date Filtering:
       - Use MongoDB date operators (`$gte`, `$lte`) on `from_date` to find upcoming or past events.
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
        
        # Strip markdown code blocks
        if raw_content.startswith("```"):
            raw_content = re.sub(r"^```(?:json)?|```$", "", raw_content).strip()
            
        print(f"\n[DEBUG - events] --- Phase 1: AI Generated Pipeline ---")
        print(raw_content)
        print("-" * 50)
        
        # Try direct JSON parsing first, fallback to extract_json_array
        try:
            return json.loads(raw_content)
        except json.JSONDecodeError:
            return extract_json_array(raw_content)
            
    except Exception as e:
        print(f"[ERROR - events] Pipeline Gen: {e}")
        return None

# --- 5. PHASE 2: EXECUTE AND EXPLAIN ---
def execute_and_explain(user_question, user_meta=None, stream=False):
    try:
        pipeline = text_to_mongo_pipeline(user_question, user_meta)
        if not pipeline:
            return {"status": False, "ai_response": "I cannot process that event query or the action is restricted."}

        db_data = []
        import pymongo.errors
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                raw_results = list(collections['events'].aggregate(pipeline))
                db_data = convert_bson_to_json(raw_results)
                break # Success
            except pymongo.errors.OperationFailure as e:
                print(f"[DEBUG - events] Attempt {attempt+1} Failed: {str(e)}. Retrying...")
                if attempt == max_retries - 1:
                    return {"status": False, "ai_response": "I cannot access that event information or the query failed."}
                
                # Ask LLM to fix the pipeline
                fix_prompt = f"The previous pipeline failed with error: {str(e)}. Provide ONLY a corrected JSON array pipeline starting with '[' and ending with ']'."
                user_question = f"{user_question}\n{fix_prompt}"
                pipeline = text_to_mongo_pipeline(user_question, user_meta)
                if not pipeline:
                     return {"status": False, "ai_response": "Failed to generate a corrected pipeline."}
        print(f"\n[DEBUG - events] --- Phase 2: Found {len(db_data)} Records ---")
        print(f"Data: {json.dumps(db_data[:2], indent=2)}")
        print("-" * 50)

        summary_prompt = f"""
        User Question: {user_question}
        Database Results: {json.dumps(db_data[:5])}
        
        CRITICAL: The database extracted the exact event records above.
        Write a lively and helpful 2-sentence summary based ONLY on the provided JSON data. 
        Mention the event name, dates/times, and either the organizers or the attendee counts (depending on what was found).
        If the results are empty [], politely state that no events matching that criteria were found.
        
        ADDITIONAL INSTRUCTION: Use very simple, easy-to-understand English. If the user is asking about their own details, address them directly using "You" or "Your" and DO NOT mention their name in the third person.
        """
        
        if not stream:

        
            summary_res = client_groq.chat.completions.create(
                messages=[{"role": "system", "content": "You are a helpful college event coordinator assistant."}, {"role": "user", "content": summary_prompt}],
                model="llama-3.1-8b-instant"
            )
            ai_summary = summary_res.choices[0].message.content.strip()
    
            print(f"\n[DEBUG - events] --- Phase 3: AI Final Summary ---")
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

        
                    messages=[{"role": "system", "content": "You are a helpful college event coordinator assistant."}, {"role": "user", "content": summary_prompt}],

        
                    model="llama-3.1-8b-instant",

        
                    stream=True

        
                )

        
                for chunk in summary_res_stream:

        
                    content = chunk.choices[0].delta.content

        
                    if content:

        
                        yield f"data: {json.dumps({'type': 'chunk', 'text': content})}\n\n"

        
            return generate()
    except Exception as e:
        return {"status": False, "ai_response": f"events Agent Error: {str(e)}"}

if __name__ == "__main__":
    print("----- events AI Agent (Phase Mode) -----")
    while True:
        q = input("\nAsk events Query: ")
        if q.lower() == "exit": break
        print(json.dumps(execute_and_explain(q), indent=2))