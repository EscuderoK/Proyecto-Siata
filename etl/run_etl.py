import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl.extract import extract_data
# Importamos la nueva funcion generate_mock_data
from etl.transform import transform_data, transform_investigadores, generate_mock_data
# Importamos la nueva funcion de carga
from etl.load import load_data_to_mysql, load_investigadores_mysql, load_simulation_to_mysql

def main():
    print("=== PIPELINE HÍBRIDO: API REAL + DATOS SINTÉTICOS ===")
    
    # 1. PARTE REAL (API)
    df_raw = extract_data()
    
    # Escuelas y Deptos
    df_escuelas, df_deptos = transform_data(df_raw)
    load_data_to_mysql(df_escuelas, df_deptos)
    
    # Investigadores
    df_inv, df_prof = transform_investigadores(df_raw, df_deptos)
    load_investigadores_mysql(df_inv, df_prof)

    # 2. PARTE SIMULADA (PYTHON GENERATOR)
    # Usamos df_inv para saber qué IDs existen y asignarlos a proyectos
    print("\n>>> INICIANDO SIMULACIÓN DE PROYECTOS Y GASTOS <<<")
    
    df_proy, df_tipo, df_vinc, df_gastos = generate_mock_data(df_inv)
    
    load_simulation_to_mysql(df_proy, df_tipo, df_vinc, df_gastos)
    
    print("\n=== PROCESO FINALIZADO CON ÉXITO ===")

if __name__ == "__main__":
    main()