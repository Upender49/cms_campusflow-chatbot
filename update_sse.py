import os
import glob
import json

dir_path = r"c:\Users\umesh\Documents\chat-bot\final-chatbot\CHAT-BOT\Backend-AI"
py_files = glob.glob(os.path.join(dir_path, "*.py"))
count = 0
for p in py_files:
    if "server.py" in p: continue
    with open(p, "r", encoding="utf-8") as f:
        content = f.read()
    
    # We want to replace exactly the specific `yield json.dumps` lines.
    old_meta = 'yield json.dumps(metadata)'
    new_meta = 'yield f"data: {json.dumps(metadata)}\\n\\n"'
    
    old_chunk = 'yield json.dumps({"type": "chunk", "text": content})'
    new_chunk = 'yield f"data: {json.dumps({\'type\': \'chunk\', \'text\': content})}\\n\\n"'
    
    modified = False
    if old_meta in content:
        content = content.replace(old_meta, new_meta)
        modified = True
    if old_chunk in content:
        content = content.replace(old_chunk, new_chunk)
        modified = True
        
    if modified:
        with open(p, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Updated {os.path.basename(p)}")
        count += 1

print(f"Finished updating {count} files.")
