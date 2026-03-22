import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
SERVER_ID = os.getenv("SERVER_ID", "co").lower().strip()

# Centralized access control — single source of truth
ALLOWED_GROUP = int(os.getenv("ALLOWED_GROUP", "-1002962582903"))
OWNER_ID = int(os.getenv("OWNER_ID", "7622959338"))

# Admin notification bot — sends hit alerts via separate bot
ADMIN_NOTIF_TOKEN = os.getenv("ADMIN_NOTIF_TOKEN", "8009433933:AAGmyxoYrfVMrPJUAGNKu0q-9K1BhPsLRAE")
ADMIN_NOTIF_CHAT = int(os.getenv("ADMIN_NOTIF_CHAT", str(OWNER_ID)))

# NopeCHA API — auto-solve hCaptcha (https://nopecha.com)
NOPECHA_KEY = os.getenv("NOPECHA_KEY", "")
