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
    "college": db["college"] # Ensure this matches your actual collection name
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

# --- 4. PHASE 1: PIPELINE GENERATOR (college FOCUS) ---
def text_to_mongo_pipeline(user_question, user_meta=None):
    # Security block removed - rely on Read-Only MongoDB User instead
    system_prompt = f"""
    You are an Expert MongoDB Architect for a college ERP system.
    TASK: Generate a MongoDB Aggregation Pipeline for the 'college' collection.
    OUTPUT: Return ONLY a valid JSON array starting with '[' and ending with ']'. NO MARKDOWN. NO ENGLISH. NO EXPLANATIONS.

    ACTIVE USER SESSION:
    - Role: {user_meta.get('role') if user_meta else 'unknown'}
    - ID/Username: {user_meta.get('username') if user_meta else 'unknown'}
    - CRITICAL RULE: If the user says "my college", "our college", or "the college", just return the standard college document. DO NOT attempt to match on any specific ID. There is only one college document in the system.
    - WARNING: NEVER generate a `$match` stage on `_id` like `{{"$match": {{"_id": "str"}}}}`. This is a placeholder and will fail.

    DYNAMIC SCHEMAS (For reference):
    college: {schemas['college']}

    CRITICAL SCHEMA PATHS FOR 'college':
    - Name & Details: `name`, `category`, `established_year`, `affiliatedTo`
    - Accreditation Object: `accreditation` (Object containing `naac_grade` and `nirf_ranking`)
      - Example path: `accreditation.nirf_ranking` (Number), `accreditation.naac_grade` (String)
    - Address Array: `address` (Array of objects containing `city`, `state`, `pin_code`, `isMainCampus`)
      - Example path: `address.city`
    - Contacts Array: `contact_persons` (Array of objects containing `first_name`, `last_name`, `email`, `phone_1`)

    AGGREGATION RULES & BEST PRACTICES:
    1. Primary Collection: The pipeline runs directly on the 'college' collection.
    2. Name & Location Matching:
       - Use `$regex` with `"$options": "i"` for string matching.
       - Query `address.city` directly using dot notation.
    3. Rankings & Numbers:
       - For `nirf_ranking` or `established_year`, use numeric comparisons.
    4. Projection:
       - ALWAYS use `$project` to return clean fields like `name`, `category`, `accreditation`, `address`, and `contact_persons`.
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
            
        print(f"\n[DEBUG - college] --- Phase 1: AI Generated Pipeline ---")
        print(raw_content)
        print("-" * 50)
        
        # Try direct JSON parsing first, fallback to extract_json_array
        try:
            return json.loads(raw_content)
        except json.JSONDecodeError:
            return extract_json_array(raw_content)
            
    except Exception as e:
        print(f"[ERROR - college] Pipeline Gen: {e}")
        return None

# --- 5. PHASE 2: EXECUTE AND EXPLAIN ---
def execute_and_explain(user_question, user_meta=None, stream=False):
    try:
        pipeline = text_to_mongo_pipeline(user_question, user_meta)
        if not pipeline:
            return {"status": False, "ai_response": "I cannot process that college query or the action is restricted."}

        db_data = []
        import pymongo.errors
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                raw_results = list(collections['college'].aggregate(pipeline))
                db_data = convert_bson_to_json(raw_results)
                break # Success
            except pymongo.errors.OperationFailure as e:
                print(f"[DEBUG - college] Attempt {attempt+1} Failed: {str(e)}. Retrying...")
                if attempt == max_retries - 1:
                    return {"status": False, "ai_response": "I cannot access that college information or the query failed."}
                
                # Ask LLM to fix the pipeline
                fix_prompt = f"The previous pipeline failed with error: {str(e)}. Provide ONLY a corrected JSON array pipeline starting with '[' and ending with ']'."
                user_question = f"{user_question}\n{fix_prompt}"
                pipeline = text_to_mongo_pipeline(user_question, user_meta)
                if not pipeline:
                     return {"status": False, "ai_response": "Failed to generate a corrected pipeline."}
        print(f"\n[DEBUG - college] --- Phase 2: Found {len(db_data)} Records ---")
        print(f"Data: {json.dumps(db_data[:2], indent=2)}")
        print("-" * 50)

        summary_prompt = f"""
        User Question: {user_question}
        Database Results: {json.dumps(db_data[:5])}
        
        TASK:
        1. If "Database Results" is NOT empty:
           - Summarize the college details found in the data (name, ranking, grade, etc.).
           - Use "You" and "Your" or "Our" to address questions about the college.
        2. If "Database Results" is empty []:
           - Politely say that no information was found about the college matching that request.
        
        STRICT RULES:
        - Max 2 sentences.
        - Friendly and professional.
        - DO NOT mention technical terms like JSON or database.
        - Use simple English.
        """
        
        if not stream:

        
            summary_res = client_groq.chat.completions.create(
                messages=[{"role": "system", "content": "You are a helpful college administration assistant."}, {"role": "user", "content": summary_prompt}],
                model="llama-3.1-8b-instant"
            )
            ai_summary = summary_res.choices[0].message.content.strip()
    
            print(f"\n[DEBUG - college] --- Phase 3: AI Final Summary ---")
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

        
                    messages=[{"role": "system", "content": "You are a helpful college administration assistant."}, {"role": "user", "content": summary_prompt}],

        
                    model="llama-3.1-8b-instant",

        
                    stream=True

        
                )

        
                for chunk in summary_res_stream:

        
                    content = chunk.choices[0].delta.content

        
                    if content:

        
                        yield f"data: {json.dumps({'type': 'chunk', 'text': content})}\n\n"

        
            return generate()
    except Exception as e:
        return {"status": False, "ai_response": f"college Agent Error: {str(e)}"}

if __name__ == "__main__":
    print("----- college AI Agent (Phase Mode) -----")
    while True:
        q = input("\nAsk college Query: ")
        if q.lower() == "exit": break
        print(json.dumps(execute_and_explain(q), indent=2))