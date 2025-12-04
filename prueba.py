from config.db_config import engine
from sqlalchemy import text

try:
    with engine.connect() as conn:
        result = conn.execute(text("SHOW TABLES;"))
        print("✔ Conexión exitosa.\nTablas encontradas:")
        for row in result:
            print("-", row[0])
except Exception as e:
    print("❌ Error:", e)
