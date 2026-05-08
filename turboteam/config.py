import os

# ==============================================================================
# КОНФІГ — ВСІ СЕКРЕТИ ТІЛЬКИ З ENVIRONMENT VARIABLES
# Якщо змінна відсутня — бот впаде при старті з чітким повідомленням.
# Це навмисна поведінка: краще не запуститись, ніж працювати з неправильними даними.
# ==============================================================================

# --- ОБОВ'ЯЗКОВІ ЗМІННІ (бот не запуститься без них) ---
BOT_TOKEN: str = os.environ["BOT_TOKEN"]
GOOGLE_SCRIPT_URL: str = os.environ["GOOGLE_SCRIPT_URL"]
REDIS_URL: str = os.environ["REDIS_URL"]

# --- ОПЦІОНАЛЬНІ З ДЕФОЛТАМИ (для локальної розробки) ---
WEB_APP_URL: str = os.getenv("WEB_APP_URL", "https://example.pythonanywhere.com/index.html")
GROUP_LINK: str = os.getenv("GROUP_LINK", "https://t.me/turboteampro")

# --- IDs (обов'язкові) ---
REPORTS_GROUP_ID: int = int(os.environ["REPORTS_GROUP_ID"])

# ADMIN_IDS — список через кому: "123456,789012"
_raw_admin_ids = os.environ.get("ADMIN_IDS", "")
ADMIN_IDS: list[int] = [int(x.strip()) for x in _raw_admin_ids.split(",") if x.strip()] if _raw_admin_ids else []

# --- HP КОНСТАНТИ (бізнес-логіка) ---
HP_GYM: int = int(os.getenv("HP_GYM", "100"))
HP_STREET: int = int(os.getenv("HP_STREET", "100"))
HP_REST: int = int(os.getenv("HP_REST", "20"))
HP_SKIP: int = int(os.getenv("HP_SKIP", "-20"))
HP_REF_BATA: int = int(os.getenv("HP_REF_BATA", "150"))
HP_REF_NEWBIE: int = int(os.getenv("HP_REF_NEWBIE", "50"))

_hp_min = int(os.getenv("HP_RANDOM_MIN", "1"))
_hp_max = int(os.getenv("HP_RANDOM_MAX", "15"))
RANDOM_HP_RANGE: tuple[int, int] = (_hp_min, _hp_max)

# --- RETRY ЛОГІКА ---
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY: float = float(os.getenv("RETRY_DELAY", "2.0"))
