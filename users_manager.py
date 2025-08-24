import json
from pathlib import Path
from datetime import datetime

USERS_FILE = "users.json"
users_data = {}

def load_users():
    global users_data
    p = Path(USERS_FILE)
    if p.exists():
        try:
            users_data = json.load(p.open("r", encoding="utf-8"))
        except Exception:
            users_data = {}
    else:
        users_data = {}

def save_users():
    p = Path(USERS_FILE)
    try:
        json.dump(users_data, p.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)
    except Exception:
        pass

def register_user(chat_id, first_name, username, language_code):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    user = users_data.get(str(chat_id))
    if user:
        # Обновляем
        user['date_last'] = now
        user['visits'] = user.get('visits', 1) + 1
        user['first_name'] = first_name
        user['username'] = username
        user['language_code'] = language_code
    else:
        users_data[str(chat_id)] = {
            "chat_id": chat_id,
            "first_name": first_name,
            "username": username,
            "language_code": language_code,
            "date_first": now,
            "date_last": now,
            "visits": 1,
        }
    save_users()

def get_all_users():
    return list(users_data.values())