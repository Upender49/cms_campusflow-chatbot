import os
import json
import re
import pymongo.errors
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

# Admin Agent has access to the main identity collections
collections = {
    "students": db["students"], # Ensure this matches your collection name
    "teachers": db["teachers"],
    "admins": db["admins"]
}

# --- 2. HELPER FUNCTIONS ---
def convert_bson_to_json(doc):
    if isinstance(doc, dict): return {k: convert_bson_to_json(v) for k, v in doc.items()}
    elif isinstance(doc, list): return [convert_bson_to_json(i) for i in doc]
    elif isinstance(doc, ObjectId): return str(doc)
    elif isinstance(doc, datetime): return doc.isoformat()
    else: return doc

def extract_json_array(text):
    """Slices the text to find only the bracketed JSON array."""
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

# --- 4. PHASE 1: PIPELINE GENERATOR (GLOBAL SEARCH MODE) ---
def text_to_mongo_pipeline(user_question, user_meta=None):
    system_prompt = f"""
    You are an Expert MongoDB Architect for a College ERP system.
    TASK: Generate a MongoDB Aggregation Pipeline to search across users.
    OUTPUT: Return ONLY a valid JSON array starting with '[' and ending with ']'. NO MARKDOWN. NO EXPLANATIONS.

    ACTIVE USER SESSION: Let the user query info about themselves.
    - Role: {user_meta.get('role') if user_meta else 'unknown'}
    - ID/Username: {user_meta.get('username') if user_meta else 'unknown'}
    - CRITICAL RULE: If the user says "my profile", "my details", "me", they want to find themselves. You MUST `$match` on their username/roll_no.

    DYNAMIC SCHEMAS (For reference):
    students: {schemas['students']}
    teachers: {schemas['teachers']}
    admins: {schemas['admins']}

    CRITICAL SCHEMA KNOWLEDGE (Shared vs Unique fields):
    - SHARED across all 3: `first_name`, `last_name`, `roll_no`, `phone_number`, `role`
    - Emails: students use `college_mail` / `person_mail`. teachers/admins use `college_email` / `personal_email`.
    - Location: students use `city`, `state`. admins/teachers use `address_line_1`, `address_line_2`.
    - Academic: students use `batch`. teachers/admins use `dept`.

    AGGREGATION RULES:
    1. The pipeline will be executed sequentially against all three collections.
    2. Primary Stage: Use `$match` to filter records. Include Role checking if the user asks explicitly for "admin", "teacher", etc.
    3. Broad Searching: Use `$or` extensively. If the user searches for a single name like "Rajesh", check BOTH `first_name` and `last_name`.
    4. Fuzzy Matching: ALWAYS use `$regex` with `"$options": "i"` for string matching.
       CRITICAL NAMES RULE: If the user provides a full name like "Rajesh Kumar", DO NOT use a single flat `$or` array for all words. That incorrectly matches ANY "Kumar". Instead, use `$and` to ensure both names match, e.g., {{"$match": {{"$and": [{{"$or": [{{"first_name": {{"$regex": "rajesh", "$options": "i"}}}}, {{"last_name": {{"$regex": "rajesh", "$options": "i"}}}}]}}, {{"$or": [{{"first_name": {{"$regex": "kumar", "$options": "i"}}}}, {{"last_name": {{"$regex": "kumar", "$options": "i"}}}}]}}]}}}}.
    5. Safe Execution: Rely on `$or` so it safely returns empty if a field (like `dept`) doesn't exist on a Student document.
    """
    try:
        response = client_groq.chat.completions.create(
            messages=[
                {"role": "system", "content": system_prompt}, 
                {"role": "user", "content": f"Create a search pipeline for this query: {user_question}"}
            ],
            model="llama-3.3-70b-versatile",
            temperature=0
        )
        raw_content = response.choices[0].message.content.strip()
        
        # Strip markdown code blocks if the LLM accidentally includes them
        if raw_content.startswith("```"):
            raw_content = re.sub(r"^```(?:json)?|```$", "", raw_content).strip()
            
        print(f"\n[DEBUG - ADMIN] --- Phase 1: Raw AI Pipeline Output ---")
        print(raw_content)
        print("-" * 50)
        
        # Try direct JSON parsing first, fallback to extract_json_array
        try:
            return json.loads(raw_content)
        except json.JSONDecodeError:
            return extract_json_array(raw_content)
            
    except Exception as e:
        print(f"[ERROR - ADMIN] Pipeline Gen: {e}")
        return None

# --- 5. PHASE 2: GLOBAL EXECUTION AND SUMMARY ---
def execute_and_explain(user_question, user_meta=None, stream=False):
    try:
        # A. Pipeline Generation
        pipeline = text_to_mongo_pipeline(user_question, user_meta)
        if not pipeline:
            return {"status": False, "ai_response": "Global search query generation failed or was blocked."}

        found_results = []
        target_collections = []
        
        # B. Sequential Search (Global Aggregation Logic)
        search_order = ["students", "teachers", "admins"]
        for col_name in search_order:
            # Safely attempt the aggregation
            try:
                results = list(collections[col_name].aggregate(pipeline))
                if results:
                    for r in results:
                        r['_source_collection'] = col_name # Track the source
                    found_results.extend(results)
                    target_collections.append(col_name)
                    # We no longer break here! We aggregate all matches.
            except pymongo.errors.OperationFailure as e:
                print(f"[DEBUG - ADMIN] Pipeline failed on {col_name}: {e}")
                continue # If the pipeline crashes on one schema, keep trying the others!
        
        target_collection = ", ".join(target_collections) if target_collections else "unknown"
        db_data = convert_bson_to_json(found_results)
        
        print(f"\n[DEBUG - ADMIN] --- Phase 2: Found Match in [{target_collection}] ---")
        print(f"[DEBUG - ADMIN] Results Count: {len(db_data)}")
        print("-" * 50)

        # C. Summary Phase
        if not db_data:
            ai_summary = "I searched all records but couldn't find anyone matching that description."
            
            # Handle empty returns safely whether streaming or not
            if stream:
                def empty_gen():
                    yield json.dumps({"type": "metadata", "status": True, "database_results": [], "database_query": pipeline})
                    yield json.dumps({"type": "chunk", "text": ai_summary})
                return empty_gen()
            else:
                return {
                    "user_question": user_question,
                    "ai_response": ai_summary,
                    "database_results": [],
                    "database_query": pipeline,
                    "source": target_collection,
                    "status": True
                }

        summary_prompt = f"""
        User asked: "{user_question}"
        We found {len(db_data)} record(s) in the '{target_collection}' collection.
        Sample Data: {json.dumps(db_data[:2])}
        
        Write a professional 2-sentence summary identifying the people found. DO NOT hallucinate info.
        
        ADDITIONAL INSTRUCTION: Use very simple, easy-to-understand English. If the user is asking about their own details, address them directly using "You" or "Your" and DO NOT mention their name in the third person.
        """
        
        if not stream:
            summary_res = client_groq.chat.completions.create(
                messages=[{"role": "system", "content": "You are a helpful database admin assistant."}, {"role": "user", "content": summary_prompt}],
                model="llama-3.1-8b-instant"
            )
            ai_summary = summary_res.choices[0].message.content.strip()

            print(f"\n[DEBUG - ADMIN] --- Phase 3: AI Final Summary ---")
            print(ai_summary)
            print("-" * 50)

            return {
                "user_question": user_question,
                "ai_response": ai_summary,
                "database_results": db_data,
                "database_query": pipeline,
                "source": target_collection,
                "status": True
            }

        else:
            def generate():
                # Yield metadata first
                metadata = {
                    "type": "metadata", 
                    "status": True,
                    "database_results": db_data,
                    "database_query": pipeline
                }
                yield f"data: {json.dumps(metadata)}\n\n"

                summary_res_stream = client_groq.chat.completions.create(
                    messages=[{"role": "system", "content": "You are a helpful database admin assistant."}, {"role": "user", "content": summary_prompt}],
                    model="llama-3.1-8b-instant",
                    stream=True
                )
                
                for chunk in summary_res_stream:
                    content = chunk.choices[0].delta.content
                    if content:
                        yield f"data: {json.dumps({'type': 'chunk', 'text': content})}\n\n"

            return generate()
            
    except Exception as e:
        return {"status": False, "ai_response": f"Admin Search Error: {str(e)}"}

if __name__ == "__main__":
    print("----- Global Admin Search Agent (Phase Mode) -----")
    while True:
        q = input("\nGlobal Search Query: ")
        if q.lower() == "exit": break
        
        # Testing non-stream response for the console
        result = execute_and_explain(q, stream=False)
        print(json.dumps(result, indent=2))