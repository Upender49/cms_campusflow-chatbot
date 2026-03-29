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

# --- SETUP MONGODB ---
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
client_groq = Groq(api_key=GROQ_API_KEY)

# Collections to search for person identity
PERSON_COLLECTIONS = {
    "student": db["students"],
    "teacher": db["teachers"],
    "admin":   db["admins"],
}

# Sensitive fields to strip before returning
RESTRICTED_FIELDS = {"password", "salary", "personal_email", "phone_number", "paying"}


# --- HELPERS ---
def convert_bson_to_json(doc):
    if isinstance(doc, dict):   return {k: convert_bson_to_json(v) for k, v in doc.items()}
    elif isinstance(doc, list): return [convert_bson_to_json(i) for i in doc]
    elif isinstance(doc, ObjectId): return str(doc)
    elif isinstance(doc, datetime): return doc.isoformat()
    else: return doc

def strip_restricted(doc):
    return {k: v for k, v in doc.items() if k not in RESTRICTED_FIELDS}


# --- PHASE 1: EXTRACT NAME FROM QUESTION ---
def extract_name_from_question(user_question):
    """Use a fast LLM call to pull the person's name out of the natural-language question."""
    prompt = """
    Extract ONLY the person's full name from the user's question.
    Return ONLY the name as a plain string. If no name is present, return "UNKNOWN".
    Do NOT return any explanation or extra words.

    Examples:
    "give me the details of Rajiv Kumar"  -> "Rajiv Kumar"
    "who is Priya Sharma?"                -> "Priya Sharma"
    "tell me about Mr. Arjun Singh"       -> "Arjun Singh"
    """
    try:
        response = client_groq.chat.completions.create(
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user",   "content": user_question}
            ],
            model="llama-3.1-8b-instant",
            temperature=0,
            max_tokens=20
        )
        name = response.choices[0].message.content.strip()
        print(f"[DEBUG - PERSON] Extracted name: '{name}'")
        return name if name.upper() != "UNKNOWN" else None
    except Exception as e:
        print(f"[ERROR - PERSON] Name extraction failed: {e}")
        return None


# --- PHASE 2: SEARCH ALL PERSON COLLECTIONS ---
def search_person_across_collections(name):
    """
    Split the name into tokens and build an $and query so that every token
    appears in either first_name or last_name — this prevents false matches
    like "Kumar" alone matching every person named Kumar.
    """
    tokens = name.strip().split()
    if not tokens:
        return []

    # Each token must match first_name OR last_name
    and_clauses = [
        {"$or": [
            {"first_name": {"$regex": tok, "$options": "i"}},
            {"last_name":  {"$regex": tok, "$options": "i"}}
        ]}
        for tok in tokens
    ]
    query = {"$and": and_clauses} if len(and_clauses) > 1 else and_clauses[0]

    all_results = []
    for role_label, collection in PERSON_COLLECTIONS.items():
        try:
            docs = list(collection.find(query))
            for doc in docs:
                doc = strip_restricted(doc)
                doc["_identified_role"] = role_label  # Tag with collection/role
                all_results.append(doc)
        except Exception as e:
            print(f"[DEBUG - PERSON] Search failed on '{role_label}': {e}")
            continue

    return convert_bson_to_json(all_results)


# --- PHASE 3: GENERATE NATURAL-LANGUAGE SUMMARY ---
def summarise_results(user_question, name, db_data):
    if not db_data:
        return f"I searched the students, teachers, and admins records but couldn't find anyone named '{name}'. Please check the spelling or try a different name."

    # Build a clean version of results to send to LLM (limit to 5)
    summary_prompt = f"""
    The user asked: "{user_question}"

    We found {len(db_data)} record(s) in our system. Here is the data:
    {json.dumps(db_data[:5])}

    Each record has a field "_identified_role" that tells you if the person is a "student", "teacher", or "admin".

    Write a clear, friendly 2–3 sentence summary that:
    - States the person's full name and their role (student / teacher / admin)
    - Mentions 1-2 key details like department, batch, roll number, or email
    - Uses simple English

    Do NOT mention technical terms like "database", "JSON", "_identified_role", or "ObjectId".
    Do NOT hallucinate any information not present in the data.
    """
    try:
        res = client_groq.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are a helpful college ERP assistant."},
                {"role": "user",   "content": summary_prompt}
            ],
            model="llama-3.1-8b-instant"
        )
        return res.choices[0].message.content.strip()
    except Exception as e:
        print(f"[ERROR - PERSON] Summary failed: {e}")
        return f"Found {len(db_data)} record(s) for '{name}' but could not generate a summary."


# --- MAIN ENTRY POINT ---
def execute_and_explain(user_question, user_meta=None, stream=False):
    try:
        print(f"\n[DEBUG - PERSON] Question: {user_question}")

        # Phase 1 — Extract name
        name = extract_name_from_question(user_question)
        if not name:
            return {
                "status": False,
                "ai_response": "I couldn't identify a person's name in your question. Could you please mention the name clearly?"
            }

        # Phase 2 — Search all collections
        db_data = search_person_across_collections(name)
        roles_found = list({doc["_identified_role"] for doc in db_data})

        print(f"[DEBUG - PERSON] Found {len(db_data)} record(s) in: {roles_found}")

        if not stream:
            # Phase 3 — Summarise
            ai_summary = summarise_results(user_question, name, db_data)

            print(f"[DEBUG - PERSON] Summary: {ai_summary}")
            print("-" * 50)

            return {
                "user_question": user_question,
                "ai_response":   ai_summary,
                "database_results": db_data,
                "source": roles_found,
                "status": True
            }
        else:
            def generate():
                import json
                metadata = {
                    "type": "metadata",
                    "user_question": user_question,
                    "database_results": db_data,
                    "source": roles_found,
                    "status": True
                }
                yield f"data: {json.dumps(metadata)}\n\n"
                
                if not db_data:
                    msg = f"I searched the students, teachers, and admins records but couldn't find anyone named '{name}'. Please check the spelling or try a different name."
                    yield f"data: {json.dumps({'type': 'chunk', 'text': msg})}\n\n"
                    return
                
                summary_prompt = f"""
    The user asked: "{user_question}"

    We found {len(db_data)} record(s) in our system. Here is the data:
    {json.dumps(db_data[:5])}

    Each record has a field "_identified_role" that tells you if the person is a "student", "teacher", or "admin".

    Write a clear, friendly 2–3 sentence summary that:
    - States the person's full name and their role (student / teacher / admin)
    - Mentions 1-2 key details like department, batch, roll number, or email
    - Uses simple English

    Do NOT mention technical terms like "database", "JSON", "_identified_role", or "ObjectId".
    Do NOT hallucinate any information not present in the data.
    """
                try:
                    res_stream = client_groq.chat.completions.create(
                        messages=[
                            {"role": "system", "content": "You are a helpful college ERP assistant."},
                            {"role": "user",   "content": summary_prompt}
                        ],
                        model="llama-3.1-8b-instant",
                        stream=True
                    )
                    for chunk in res_stream:
                        content = chunk.choices[0].delta.content
                        if content:
                            yield f"data: {json.dumps({'type': 'chunk', 'text': content})}\n\n"
                except Exception as e:
                    msg = f"Found {len(db_data)} record(s) for '{name}' but could not generate a summary."
                    yield f"data: {json.dumps({'type': 'chunk', 'text': msg})}\n\n"
            
            return generate()

    except Exception as e:
        return {"status": False, "ai_response": f"Person Agent Error: {str(e)}"}


# --- STANDALONE TEST ---
if __name__ == "__main__":
    print("----- Person Identity Agent -----")
    while True:
        q = input("\nAsk (or 'exit'): ")
        if q.lower() == "exit":
            break
        print(json.dumps(execute_and_explain(q), indent=2))
