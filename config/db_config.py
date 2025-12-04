
from sqlalchemy import create_engine

# --- 1. CONFIGURACIÓN DE BASE DE DATOS MYSQL ---
DB_USER = 'root'
DB_PASS = '***123'  
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'siata_db'

# --- 2. CONFIGURACIÓN DE LA API (ESTO ES LO QUE TE FALTA) ---
# Copia y pega estas dos líneas tal cual (con tus claves)
SOCRATA_API_KEY_ID = "dh0g19jmuwv14***"      
SOCRATA_API_SECRET = "2zsina0b3iobwbpzcgrnev6h***" 

# --- 3. FUNCIÓN DE CONEXIÓN ---
def get_db_engine():
    """Crea y devuelve el motor de conexión a MySQL"""
    connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)
    return engine
