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
    "students": db["students"],
    "academics": db["academics"],
    "subjects": db["subjects"],
    "results": db["results"], 
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

# --- 4. PHASE 1: PIPELINE GENERATOR ---
# --- 4. PHASE 1: PIPELINE GENERATOR ---
def text_to_mongo_pipeline(user_question, user_meta=None):
    # Security block removed - rely on Read-Only MongoDB User instead
    system_prompt = f"""
    You are an Expert MongoDB Architect for a College ERP system.
    TASK: Generate a MongoDB Aggregation Pipeline for the 'students' collection.
    OUTPUT: Return ONLY a valid JSON array starting with '[' and ending with ']'. NO MARKDOWN. NO ENGLISH. NO EXPLANATIONS.
    
    ACTIVE USER SESSION: Let the user query info about themselves.
    - Role: {user_meta.get('role') if user_meta else 'unknown'}
    - ID/Username: {user_meta.get('username') if user_meta else 'unknown'}
    - CRITICAL RULE: If the user says "my", "mine", "I", "me", you MUST add a `$match` stage checking their Role ID against the relevant collection document (e.g. `$match: {{"roll_no": "{user_meta.get('username') if user_meta else ''}"}}`).

    DYNAMIC SCHEMAS (For reference):
    students: {schemas['students']}
    academics: {schemas['academics']}
    subjects: {schemas['subjects']}
    results: {schemas['results']}

    CRITICAL SCHEMA PATHS & QUIRKS (MUST FOLLOW EXACTLY):
    - Primary Collection: 'students'
    - Identifiers: `first_name`, `last_name`, `roll_no`, `batch`, `college_mail`, `person_mail`
    - References on Student: `academic` (Ref academics), `results` (Ref results)
    
    ACADEMIC & RESULT NESTING STRUCTURE:
    - `years` (Array)
    - `years.semesters` (OBJECT - DO NOT UNWIND THIS)
    - `years.semesters.registerd_subjects` (ARRAY - Note the spelling: 'registerd_subjects')

    AGGREGATION RULES & BEST PRACTICES:
    1. Base Search:
       - Match `first_name`, `last_name`, or `roll_no` using `$regex` with `"$options": "i"`.
       - Example: {{"$match": {{"$or": [{{"first_name": {{"$regex": "name", "$options": "i"}}}}, {{"last_name": {{"$regex": "name", "$options": "i"}}}}]}}}}
    2. Joining academics or results:
       - Lookup 'academics' using localField: "academic", foreignField: "_id", as: "academic_docs".
       - Lookup 'results' using localField: "results", foreignField: "_id", as: "result_docs".
       - Use `$unwind: {{ path: "$academic_docs", preserveNullAndEmptyArrays: true }}` (Same for result_docs).
    3. Accessing Nested Data (CRITICAL UNWIND ORDER):
       - To get marks, grades, or attendance, you MUST unwind the arrays sequentially.
       - 1st: `$unwind: "$result_docs.years"` (or academic_docs.years)
       - 2nd: `$unwind: "$result_docs.years.semesters.registerd_subjects"`
       - NEVER unwind `semesters`.
    4. Joining subjects:
       - After unwinding `registerd_subjects`, `$lookup` 'subjects' using localField: "result_docs.years.semesters.registerd_subjects.subject_id".
    5. Clean Output:
       - ALWAYS use a final `$project` stage to return clean fields like `first_name`, `last_name`, `roll_no`, and whatever specific data (marks, subjects, etc.) the user requested.
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
            
        # DEBUG PRINT
        print(f"\n[DEBUG] --- Phase 1: Raw AI Pipeline Output ---")
        print(raw_content)
        print("-" * 50)
        
        # Try direct JSON parsing first, fallback to extract_json_array
        try:
            return json.loads(raw_content)
        except json.JSONDecodeError:
            return extract_json_array(raw_content)
            
    except Exception as e:
        print(f"[ERROR] Pipeline Gen: {e}")
        return None

# --- 5. PHASE 2: EXECUTE AND EXPLAIN ---
def execute_and_explain(user_question, user_meta=None, stream=False):
    try:
        # A. Pipeline Phase
        pipeline = text_to_mongo_pipeline(user_question, user_meta)
        if not pipeline:
            return {"status": False, "ai_response": "The AI failed to generate a valid database query."}

        # B. Database Phase with Self-Healing Retry Loop
        db_data = []
        import pymongo.errors
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                raw_results = list(collections['students'].aggregate(pipeline))
                db_data = convert_bson_to_json(raw_results)
                break # Success
            except pymongo.errors.OperationFailure as e:
                print(f"[DEBUG] Attempt {attempt+1} Failed: {str(e)}. Retrying...")
                if attempt == max_retries - 1:
                    return {"status": False, "ai_response": f"Database error after retries: {str(e)}"}
                
                # Ask LLM to fix the pipeline
                fix_prompt = f"The previous pipeline failed with error: {str(e)}. Provide ONLY a corrected JSON array pipeline starting with '[' and ending with ']'."
                user_question = f"{user_question}\n{fix_prompt}"
                pipeline = text_to_mongo_pipeline(user_question, user_meta)
                if not pipeline:
                     return {"status": False, "ai_response": "Failed to generate a corrected pipeline."}

        
        # DEBUG PRINT
        print(f"\n[DEBUG] --- Phase 2: Database results Count: {len(db_data)} ---")
        print(f"[DEBUG] Sample Result: {json.dumps(db_data[:1], indent=2)}")
        print("-" * 50)

        # C. Summary Phase
        summary_prompt = f"""
        User Question: {user_question}
        Database Data: {json.dumps(db_data[:5])}
        
        TASK:
        1. If "Database Data" is NOT empty:
           - Summarize the specific details found in the data that answer the "User Question".
           - Use "You" and "Your" to address the user directly.
           - DO NOT say "No records found".
        2. If "Database Data" is empty []:
           - Politely say that no records were found matching their request.
        
        STRICT RULES:
        - Max 2 sentences.
        - Friendly and professional.
        - DO NOT mention "Database", "JSON", "Python", or any technical terms.
        - Use simple English.
        """
        
        if not stream:

        
            summary_res = client_groq.chat.completions.create(
                messages=[{"role": "system", "content": "You are a helpful college assistant."}, {"role": "user", "content": summary_prompt}],
                model="llama-3.1-8b-instant"
            )
            
            ai_final_text = summary_res.choices[0].message.content.strip()
    
            # DEBUG PRINT
            print(f"\n[DEBUG] --- Phase 3: AI Final Summary ---")
            print(ai_final_text)
            print("-" * 50)
    
            return {
                "user_question": user_question,
                "ai_response": ai_final_text,
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
        return {"status": False, "ai_response": f"Runtime Error: {str(e)}"}

if __name__ == "__main__":
    print("----- students Database AI Agent (Two-Phase Debug Mode) -----")
    while True:
        q = input("\nAsk question (or exit): ")
        if q.lower() == "exit": break
        final_output = execute_and_explain(q)
        print("\n[FINAL JSON FOR FRONTEND]")
        print(json.dumps(final_output, indent=2))