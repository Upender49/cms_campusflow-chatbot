import os
import json
from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
from groq import Groq
from dotenv import load_dotenv


import student    # Hub for basic details/demographics
import teacher    # Faculty info and mentor relations
import admin      # Global search fallback
import academic   # Branches, sections, and semesters
import attendance # Present/absent records
import results    # Marks, grades, and GPA
import college    # college profile, accreditation, and location
import events     # Campus events and registrations
import timetable  # Class schedules and time-slots
import upload
import person     # Cross-collection person identity lookup
load_dotenv()
app = Flask(__name__)
CORS(app)


GROQ_API_KEY = os.getenv("GROQ_API_KEY")
client = Groq(api_key=GROQ_API_KEY)

def is_general_chat(user_question):
    check_prompt = """
    You are a strict Binary Classifier.
    Task: Classify if the user input is "CHAT" or "QUERY".
    - CHAT: Greetings, "How are you?", "Who made you?", Thanks, or casual conversation.
    - QUERY: Requests requiring database lookups (names, dates, events, marks, schedules).
    OUTPUT: ONLY the word "CHAT" or "QUERY".
    """
    try:
        response = client.chat.completions.create(
            messages=[{"role": "system", "content": check_prompt}, {"role": "user", "content": user_question}],
            model="llama-3.1-8b-instant", temperature=0, max_tokens=5 
        )
        return response.choices[0].message.content.strip().upper() == "CHAT"
    except: return False

def handle_general_response(user_question):
    system_prompt = "You are the 'college ERP AI Guide'. Be helpful, witty, and concise. Do not guess data."
    try:
        response_stream = client.chat.completions.create(
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_question}],
            model="llama-3.1-8b-instant", temperature=0.7, stream=True
        )
        metadata = {
            "type": "metadata",
            "agent": "GENERAL_AGENT",
            "rewritten_query": user_question,
            "status": True
        }
        yield f"data: {json.dumps(metadata)}\n\n"
        
        for chunk in response_stream:
            content = chunk.choices[0].delta.content
            if content:
                yield f"data: {json.dumps({'type': 'chunk', 'text': content})}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'type': 'chunk', 'text': 'I am here to help with your college ERP. What would you like to know?'})}\n\n"

def rewrite_query_with_context(user_query, history):
    if not history or len(history) == 0:
        return user_query
    
    # Format history for the prompt
    history_text = "\n".join([f"User: {msg.get('user', '')}\nAI: {msg.get('ai', '')}" for msg in history[-3:]])
    
    system_prompt = f"""
    You are a STRICT Query Rewriter for a College ERP. Your ONLY job is to make the user's latest question self-contained.

    ABSOLUTE RULES (NEVER BREAK THESE):
    1. NEVER answer the question. Return ONLY a rewritten question.
    2. PRESERVE INTENT 100%: The rewritten query must have the EXACT SAME meaning as the original.
       - If user asks "is rajesh kumar admin?", output MUST still ask about admin status. NEVER simplify to "Who is Rajesh Kumar?".
    3. TOPIC SHIFT = NO CONTEXT INJECTION:
       - SELF queries contain "my", "me", "I", "mine". You MAY use prior context (e.g. subject names) ONLY for SELF queries.
       - THIRD-PARTY queries mention a specific name or role ("the admin", "the teacher", "Rajesh Kumar"). NEVER inject logged-in user's info into these.
    4. PRONOUN RESOLUTION: Only resolve vague pronouns when OBVIOUS from immediate prior history AND same topic.
    5. IF IN DOUBT: Return the query EXACTLY as written.

    GOOD EXAMPLES:
    - "is rajesh kumar admin?" → "Is Rajesh Kumar an admin?"
    - "then give my marks" (after asking about subjects) → "What are my marks for [previously mentioned subjects]?"

    BAD EXAMPLES (NEVER DO THESE):
    - "is rajesh kumar admin?" → "Who is Rajesh Kumar?" (WRONG: Lost the admin intent!)
    - "give my marks" → "Your attendance is 2 out of 3." (WRONG: This is an answer!)

    CONVERSATION HISTORY:
    {history_text}
    """
    
    try:
        response = client.chat.completions.create(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Rewrite this query to be standalone (return ONLY the rewritten question, no explanations): {user_query}"}
            ],
            model="llama-3.1-8b-instant", temperature=0
        )
        rewritten_query = response.choices[0].message.content.strip()
        # Safety fallback: discard rewrite if it looks like an answer (too long)
        if len(rewritten_query) > len(user_query) * 4 and len(user_query) > 10:
            print(f"[Context Rewriter] SAFETY FALLBACK: Rewrite too long, using original.")
            return user_query
        print(f"[Context Rewriter] Original: '{user_query}' | Rewritten: '{rewritten_query}'")
        return rewritten_query
    except Exception as e:
        print(f"[Context Rewriter Error] {str(e)}")
        return user_query

def route_query(user_question):
    # --- ENHANCED DECISION MATRIX V5.0 ---
    system_prompt = """
    You are the Master Intent Router for a College ERP.
    Categorize the user's question into EXACTLY ONE agent.

    DECISION LOGIC:
    1. RESULTS_AGENT: Marks, Grades, GPA, Pass/Fail status, specific exam scores.
    2. ATTENDANCE_AGENT: Percentage, present/absent count, day-by-day status.
    3. TIMETABLE_AGENT: "What class is now?", "When is the next lecture?", "Monday schedule", "time-slots".
    4. EVENTS_AGENT: "What events are happening?", "Hackathons", "Cultural fests", "Who is registered for the event?".
    5. COLLEGE_AGENT: NAAC grade, NIRF ranking, college address, campus location, principal/dean info.
    6. ACADEMIC_AGENT: Semesters, branch/section details, assigned mentor name, class teacher.
    7. TEACHER_AGENT: Specific faculty info — ONLY when the query EXPLICITLY mentions "teacher", "faculty", "professor", or "lecturer".
    8. UPLOAD_AGENT: Files, documents, marksheets, images, PDFs, "Who uploaded this file?", "Show me my documents".
    9. STUDENT_AGENT: Student demographics (City, Phone, DOB, Gender, Admission type, Roll No) — ONLY when the query EXPLICITLY mentions "student".
    10. PERSON_AGENT: "Who is X?", "Give me details of X", "Tell me about X", "What is X's role?" — when a PERSON'S NAME is mentioned but NO role (student/teacher/admin) is explicitly stated. This is the default for any identity query about a named individual.
    11. ADMIN_AGENT: Global vague searches with no name (e.g., "Search all admins", "Email check") or cross-collection queries without a specific person.

    CRITICAL RULE: If the user mentions a person's name WITHOUT specifying their role, ALWAYS choose PERSON_AGENT — never default to STUDENT_AGENT.

    OUTPUT ONLY THE EXACT CATEGORY NAME.
    """
    try:
        response = client.chat.completions.create(
            messages=[
                {"role": "system", "content": system_prompt}, 
                {"role": "user", "content": user_question}
            ],
            model="llama-3.3-70b-versatile", 
            temperature=0
        )
        res = response.choices[0].message.content.strip().upper()
        
        # Mapping to ensure clean strings — order matters: PERSON before STUDENT/TEACHER
        mapping = [
            ("RESULTS",    "RESULTS_AGENT"),
            ("ATTENDANCE", "ATTENDANCE_AGENT"),
            ("TIMETABLE",  "TIMETABLE_AGENT"),
            ("EVENTS",     "EVENTS_AGENT"),
            ("COLLEGE",    "COLLEGE_AGENT"),
            ("ACADEMIC",   "ACADEMIC_AGENT"),
            ("UPLOAD",     "UPLOAD_AGENT"),
            ("PERSON",     "PERSON_AGENT"),   # Check PERSON before STUDENT/TEACHER
            ("TEACHER",    "TEACHER_AGENT"),
            ("STUDENT",    "STUDENT_AGENT"),
        ]
        for key, value in mapping:
            if key in res: return value
        return "ADMIN_AGENT"
    except: return "ADMIN_AGENT"

@app.route('/ai/chat/college', methods=['POST'])
def chat():
    try:
        data = request.json
        user_q = data.get('message') or data.get('query')
        history = data.get('history', []) # Extract conversation history (Array of dicts: [{user: "...", ai: "..."}, ...])
        active_user = data.get('user', {"username": "unknown", "role": "unknown"}) # Extract LocalStorage session details
        
        if not user_q: return jsonify({"error": "Empty message"}), 400

        print(f"\n[Incoming]: {user_q}")

        if is_general_chat(user_q):
            print("[Router]: Type -> GENERAL")
            return Response(stream_with_context(handle_general_response(user_q)), mimetype='text/event-stream')
        
        # Rewrite query using conversation memory
        contextual_query = rewrite_query_with_context(user_q, history)

        # Multi-Intent Analysis
        intent_check_prompt = """
        Analyze if this question is a SINGLE intent or a DOUBLE intent question crossing multiple domains.
        Output ONLY "SINGLE" or "DOUBLE".
        Example SINGLE: "What is John's attendance?"
        Example DOUBLE: "What is John's attendance and his marks?"
        """
        try:
            intent_res = client.chat.completions.create(
                messages=[{"role": "system", "content": intent_check_prompt}, {"role": "user", "content": contextual_query}],
                model="llama-3.1-8b-instant", temperature=0
            )
            intent_type = intent_res.choices[0].message.content.strip().upper()
        except:
            intent_type = "SINGLE"

        # --- EXECUTION ENGINE ---
        def run_agent(agent_choice, q, user_meta, stream=False):
            if agent_choice == "RESULTS_AGENT":     return results.execute_and_explain(q, user_meta, stream=stream)
            elif agent_choice == "ATTENDANCE_AGENT": return attendance.execute_and_explain(q, user_meta, stream=stream)
            elif agent_choice == "TIMETABLE_AGENT":  return timetable.execute_and_explain(q, user_meta, stream=stream)
            elif agent_choice == "EVENTS_AGENT":     return events.execute_and_explain(q, user_meta, stream=stream)
            elif agent_choice == "COLLEGE_AGENT":    return college.execute_and_explain(q, user_meta, stream=stream)
            elif agent_choice == "ACADEMIC_AGENT":   return academic.execute_and_explain(q, user_meta, stream=stream)
            elif agent_choice == "TEACHER_AGENT":    return teacher.execute_and_explain(q, user_meta, stream=stream)
            elif agent_choice == "STUDENT_AGENT":    return student.execute_and_explain(q, user_meta, stream=stream)
            elif agent_choice == "UPLOAD_AGENT":     return upload.execute_and_explain(q, user_meta, stream=stream)
            elif agent_choice == "PERSON_AGENT":     return person.execute_and_explain(q, user_meta, stream=stream)
            else:                                    return admin.execute_and_explain(q, user_meta, stream=stream)

        if "DOUBLE" in intent_type:
            print("[Router]: MULTI-INTENT DETECTED")
            split_prompt = f"""
            Split this compound question into EXACTLY TWO distinct questions.
            Return a valid JSON array of two strings.
            Example Question: "What is John's attendance and his marks?"
            Output: ["What is John's attendance?", "What are John's marks?"]
            
            Question: {contextual_query}
            """
            try:
                split_res = client.chat.completions.create(
                    messages=[{"role": "user", "content": split_prompt}],
                    model="llama-3.1-8b-instant", temperature=0
                )
                import re, json
                raw = split_res.choices[0].message.content.strip()
                if raw.startswith("```"): raw = re.sub(r"^```(?:json)?|```$", "", raw).strip()
                queries = json.loads(raw)
            except:
                queries = [contextual_query] # Fallback to single if fail

            if len(queries) == 2:
                q1, q2 = queries[0], queries[1]
                agent1, agent2 = route_query(q1), route_query(q2)
                
                print(f"[Router]: Sub-Query 1 -> {agent1} ({q1})")
                print(f"[Router]: Sub-Query 2 -> {agent2} ({q2})")
                
                res1 = run_agent(agent1, q1, active_user, stream=False)
                res2 = run_agent(agent2, q2, active_user, stream=False)
                
                # Grand Summary
                merge_prompt = f"""
                You are a College AI Assistant. You were asked: "{user_q}"
                
                Data Source 1: {res1.get('ai_response', 'No data')}
                Data Source 2: {res2.get('ai_response', 'No data')}
                
                Write a single, cohesive, friendly response combining both pieces of information naturally.
                """
                
                merged_table = []
                if res1.get('database_results'): merged_table.extend(res1['database_results'])
                if res2.get('database_results'): merged_table.extend(res2['database_results'])

                def multi_agent_generator():
                    metadata = {
                        "type": "metadata",
                        "agent": "MULTI_AGENT",
                        "rewritten_query": contextual_query,
                        "database_results": merged_table[:10],
                        "status": True
                    }
                    yield f"data: {json.dumps(metadata)}\n\n"
                    
                    summary_res_stream = client.chat.completions.create(
                        messages=[{"role": "user", "content": merge_prompt}],
                        model="llama-3.1-8b-instant", temperature=0.5, stream=True
                    )
                    for chunk in summary_res_stream:
                        content = chunk.choices[0].delta.content
                        if content:
                            yield f"data: {json.dumps({'type': 'chunk', 'text': content})}\n\n"

                return Response(stream_with_context(multi_agent_generator()), mimetype='text/event-stream')

        # Single Query Execution Fallback
        agent_choice = route_query(contextual_query)
        print(f"[Router]: Route -> {agent_choice} | Active User -> {active_user.get('username')} [{active_user.get('role')}]")
        agent_generator = run_agent(agent_choice, contextual_query, active_user, stream=True)

        # Handle early dictionary returns from agents (errors)
        if isinstance(agent_generator, dict):
            def dict_to_stream(d):
                yield f"data: {json.dumps({'type': 'metadata', 'status': d.get('status', False)})}\n\n"
                if 'ai_response' in d:
                    yield f"data: {json.dumps({'type': 'chunk', 'text': d['ai_response']})}\n\n"
            agent_generator = dict_to_stream(agent_generator)

        return Response(stream_with_context(agent_generator), mimetype='text/event-stream')

    except Exception as e:
        print(f"[Critical Error]: {str(e)}")
        return jsonify({"error": "Internal Server Error"}), 500

if __name__ == '__main__':
    print("--- college ERP AI MASTER ROUTER (V3.0) ACTIVE ---")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)