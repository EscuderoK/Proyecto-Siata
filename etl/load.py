import pandas as pd
from sqlalchemy import text
import sys
import os

# Ajuste de rutas para importar la configuración
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.db_config import get_db_engine

def load_data_to_mysql(df_escuelas, df_departamentos):
    """Carga Escuelas y Departamentos"""
    print("--- [L] LOAD: Escribiendo Estructura Académica... ---")
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            # Limpiar tablas (Opcional, cuidado en producción)
            print("--> Limpiando tablas base...")
            conn.execute(text("DELETE FROM Departamento"))
            conn.execute(text("DELETE FROM Escuela"))
            conn.commit()

        df_escuelas.to_sql('Escuela', con=engine, index=False, if_exists='append')
        print(f"--> {len(df_escuelas)} registros insertados en 'Escuela'.")

        df_departamentos.to_sql('Departamento', con=engine, index=False, if_exists='append')
        print(f"--> {len(df_departamentos)} registros insertados en 'Departamento'.")

    except Exception as e:
        print(f"!!! ERROR CARGA ESTRUCTURA: {e}")

def load_investigadores_mysql(df_inv, df_prof):
    """Carga Investigadores y Profesores"""
    print("--- [L] LOAD: Cargando Personas... ---")
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            # Limpiar tablas para evitar errores de duplicados en pruebas
            conn.execute(text("DELETE FROM Profesor")) 
            conn.execute(text("DELETE FROM Investigador WHERE tipo_investigador = 'Profesor'"))
            conn.commit()

        df_inv.to_sql('Investigador', con=engine, index=False, if_exists='append')
        print(f"--> {len(df_inv)} registros insertados en 'Investigador'.")

        df_prof.to_sql('Profesor', con=engine, index=False, if_exists='append')
        print(f"--> {len(df_prof)} registros insertados en 'Profesor'.")

    except Exception as e:
        print(f"!!! ERROR CARGA PERSONAS: {e}")

def load_simulation_to_mysql(df_proy, df_tipo, df_vinc, df_gastos):
    """Carga la Simulación Masiva de Proyectos y Gastos"""
    print("--- [L] LOAD: Cargando Simulación Masiva... ---")
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            # Limpieza (Orden inverso por llaves foráneas)
            print("--> Limpiando tablas de simulación...")
            conn.execute(text("DELETE FROM Gasto_Ejecutado"))
            conn.execute(text("DELETE FROM Vinculacion_Proyecto"))
            conn.execute(text("DELETE FROM Proyecto"))
            conn.execute(text("DELETE FROM Tipo_Gasto"))
            conn.commit()

        # Cargar en orden estricto
        df_tipo.to_sql('Tipo_Gasto', con=engine, index=False, if_exists='append')
        print(f"--> {len(df_tipo)} Tipos de Gasto cargados.")

        df_proy.to_sql('Proyecto', con=engine, index=False, if_exists='append')
        print(f"--> {len(df_proy)} Proyectos cargados.")
        
        df_vinc.to_sql('Vinculacion_Proyecto', con=engine, index=False, if_exists='append')
        print(f"--> {len(df_vinc)} Vinculaciones cargadas.")
        
        df_gastos.to_sql('Gasto_Ejecutado', con=engine, index=False, if_exists='append')
        print(f"--> {len(df_gastos)} Gastos Ejecutados cargados.")
        
        print("--> ¡Simulación cargada exitosamente en MySQL!")

    except Exception as e:
        print(f"!!! ERROR CARGA SIMULACION: {e}")