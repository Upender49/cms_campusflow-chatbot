import student
import attendance
import college
import results
import timetable
import json

user_meta = {"username": "ME2022015", "role": "student"}

def test_query(agent_name, agent_mod, query):
    print(f"\n--- Testing {agent_name} ---")
    print(f"Query: {query}")
    try:
        # We only check if it runs without error and generates a pipeline
        # since we don't want to rely on the actual DB state for everything
        res = agent_mod.execute_and_explain(query, user_meta)
        print(f"Status: {res.get('status')}")
        print(f"AI Response: {res.get('ai_response')}")
        print(f"Pipeline: {json.dumps(res.get('database_query'), indent=2)}")
    except Exception as e:
        print(f"Error: {e}")

# 1. Student Agent Test (Personal Details)
test_query("Student", student, "What is my personal email id and what city am I from?")

# 2. Attendance Agent Test (Self Filtering)
test_query("Attendance", attendance, "Tell me my attendance for Data Structures and Algorithms")

# 3. College Agent Test (Rankings)
test_query("College", college, "What is our college's NIRF ranking and NAAC grade?")

# 4. Results Agent Test (Standardization)
test_query("Results", results, "Show me my marks for Data Structures")

# 5. Timetable Agent Test (Standardization)
test_query("Timetable", timetable, "What is my schedule for Monday?")
