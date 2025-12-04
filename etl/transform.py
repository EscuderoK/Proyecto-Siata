import pandas as pd
import numpy as np

import random
from datetime import date, timedelta

def transform_data(df_raw):
    """
    1. Transforma Escuelas y Departamentos.
    Genera IDs explícitos para asegurar integridad referencial.
    """
    print("--- [T] TRANSFORMATION: Normalizando Escuelas y Deptos... ---")

    if df_raw is None or df_raw.empty:
        raise ValueError("DataFrame vacío.")

    col_gran_area = 'NME_GRAN_AREA_PR'
    col_area = 'NME_AREA_PR'

    # --- 1. ESCUELAS ---
    df_escuelas = df_raw[[col_gran_area]].drop_duplicates().dropna()
    df_escuelas = df_escuelas.rename(columns={col_gran_area: 'nombre_escuela'})
    df_escuelas['nombre_escuela'] = df_escuelas['nombre_escuela'].str.strip().str.title()
    # Generamos ID explícito
    df_escuelas['id_escuela'] = range(1, len(df_escuelas) + 1)

    print(f"--> Escuelas identificadas: {len(df_escuelas)}")

    # --- 2. DEPARTAMENTOS ---
    df_deptos = df_raw[[col_gran_area, col_area]].drop_duplicates().dropna()
    df_deptos[col_gran_area] = df_deptos[col_gran_area].str.strip().str.title()
    df_deptos[col_area] = df_deptos[col_area].str.strip().str.title()

    # Mapear ID Escuela
    mapa_escuelas = pd.Series(df_escuelas.id_escuela.values, index=df_escuelas.nombre_escuela).to_dict()
    df_deptos['id_escuela'] = df_deptos[col_gran_area].map(mapa_escuelas)

    # Renombrar columna departamento
    df_deptos = df_deptos.rename(columns={col_area: 'nombre_departamento'})

    # === CORRECCIÓN CLAVE AQUÍ ===
    # Generamos el ID del departamento AQUÍ en Python, no en MySQL.
    # Así sabemos exactamente qué numero es cada depto.
    df_deptos['id_departamento'] = range(1, len(df_deptos) + 1)
    # =============================

    # Seleccionamos las columnas incluyendo el ID generado
    df_deptos_final = df_deptos[['id_departamento', 'nombre_departamento', 'id_escuela']]

    print(f"--> Departamentos procesados: {len(df_deptos_final)}")

    return df_escuelas, df_deptos_final

def transform_investigadores(df_raw, df_deptos_con_ids):
    """
    2. Transforma Investigadores cruzando con los IDs reales de departamentos.
    """
    print("\n--- [T] TRANSFORM: Procesando Investigadores... ---")

    col_id_persona = 'ID_PERSONA_PR'
    col_municipio = 'NME_MUNICIPIO_NAC_PR'
    col_categoria = 'NME_CLASIFICACION_PR'
    col_area = 'NME_AREA_PR'
    
    # Quitamos duplicados por persona
    cols_necesarias = [col_id_persona, col_municipio, col_categoria, col_area]
    df_profs = df_raw[cols_necesarias].drop_duplicates(subset=[col_id_persona]).copy()

    # --- TABLA PADRE: INVESTIGADOR ---
    df_profs['id_investigador'] = df_profs[col_id_persona]
    df_profs['nombre_completo'] = "Investigador " + df_profs[col_id_persona].astype(str)
    df_profs['tipo_investigador'] = 'Profesor'

    df_tabla_investigador = df_profs[['id_investigador', 'nombre_completo', 'tipo_investigador']]

    # --- TABLA HIJA: PROFESOR ---
    df_profs['ciudad_nacimiento'] = df_profs[col_municipio].fillna('Desconocida').str.title()

    # Salario
    def calcular_salario(clasificacion):
        cat = str(clasificacion).upper()
        if 'SENIOR' in cat: return 9000000
        elif 'ASOCIADO' in cat: return 7000000
        elif 'JUNIOR' in cat: return 5000000
        else: return 4000000 
    
    df_profs['salario'] = df_profs[col_categoria].apply(calcular_salario)

    # --- CRUCE EXACTO DE DEPARTAMENTOS ---
    # Normalizamos el nombre del área para asegurar coincidencia
    df_profs[col_area] = df_profs[col_area].str.strip().str.title()
    
    # Creamos el mapa usando los IDs REALES que generamos en la función anterior
    # df_deptos_con_ids ahora tiene la columna 'id_departamento' explícita
    mapa_deptos = pd.Series(
        df_deptos_con_ids.id_departamento.values, 
        index=df_deptos_con_ids.nombre_departamento
    ).to_dict()
    
    # Mapeamos
    df_profs['id_departamento'] = df_profs[col_area].map(mapa_deptos)
    
    # --- MANEJO DE ERRORES DE CRUCE ---
    # Si algún departamento no cruzó (NaN), asignamos el ID 1 por defecto.
    # Como generamos IDs desde el 1, aseguramos que el ID 1 SIEMPRE EXISTE.
    df_profs['id_departamento'] = df_profs['id_departamento'].fillna(1).astype(int)

    # Datos Dummy
    df_profs['grupo_investigacion'] = 'Grupo Generico TIFAE'
    df_profs['fecha_nacimiento'] = '1985-06-15'
    df_profs['ciudad_residencia'] = 'Bogotá'
    df_profs['direccion_residencia'] = 'Carrera 7 # 40-50'

    columnas_prof = ['id_investigador', 'ciudad_nacimiento', 'fecha_nacimiento', 
                     'ciudad_residencia', 'direccion_residencia', 'salario', 
                     'grupo_investigacion', 'id_departamento']
    
    df_tabla_profesor = df_profs[columnas_prof]

    print(f"--> Investigadores listos: {len(df_tabla_investigador)}")

    return df_tabla_investigador, df_tabla_profesor


def generate_mock_data(df_investigadores_creados):
    """
    Genera datos sintéticos para Proyectos, Gastos y Vinculaciones.
    Requiere el DataFrame de investigadores ya creado para usar sus IDs reales.
    """
    print("\n--- [GENERADOR] Creando Datos Sintéticos (Mock Data)... ---")
    
    # 1. GENERAR PROYECTOS (Tabla Maestra)
    # ------------------------------------
    num_proyectos = 50
    codigos_proy = [f"PRY-{100+i}" for i in range(num_proyectos)]
    
    data_proy = {
        'codigo_proyecto': codigos_proy,
        'nombre_proyecto': [f"Proyecto de Investigación TIFAE Fase {i}" for i in range(num_proyectos)],
        'fecha_inicio': [date(2022, 1, 1) + timedelta(days=random.randint(0, 365)) for _ in range(num_proyectos)]
    }
    df_proyectos = pd.DataFrame(data_proy)
    # La fecha fin es inicio + 1 año aprox
    df_proyectos['fecha_fin'] = df_proyectos['fecha_inicio'] + timedelta(days=365)
    
    print(f"--> Simulados: {len(df_proyectos)} Proyectos.")

    # 2. GENERAR TIPOS DE GASTO (Tabla Referencia)
    # --------------------------------------------
    data_gastos = {
        'descripcion_gasto': ['Papelería', 'Viáticos Nacionales', 'Software Licenciado', 'Equipos de Cómputo', 'Salidas de Campo', 'Insumos de Laboratorio'],
        'valor_estandar': [50000, 250000, 1500000, 4500000, 80000, 120000]
    }
    df_tipo_gasto = pd.DataFrame(data_gastos)
    df_tipo_gasto['id_tipo_gasto'] = range(1, len(df_tipo_gasto) + 1) # IDs 1 al 6
    
    print(f"--> Simulados: {len(df_tipo_gasto)} Tipos de Gasto.")

    # 3. GENERAR VINCULACIÓN (Tabla Muchos a Muchos)
    # ----------------------------------------------
    # Lógica: Cada proyecto tendrá entre 3 y 8 investigadores aleatorios
    vinculaciones = []
    
    ids_inv_disponibles = df_investigadores_creados['id_investigador'].tolist()
    
    for codigo in df_proyectos['codigo_proyecto']:
        # Elegimos entre 3 y 8 investigadores al azar para este proyecto
        participantes = random.sample(ids_inv_disponibles, k=random.randint(3, 8))
        
        for id_inv in participantes:
            rol = random.choice(['Investigador Principal', 'Co-Investigador', 'Asistente', 'Analista'])
            vinculaciones.append({
                'codigo_proyecto': codigo,
                'id_investigador': id_inv,
                'rol_proyecto': rol,
                'horas_asignadas': random.choice([10, 20, 40]),
                'fecha_inicio_vinculacion': date(2022, 2, 1),
                'fecha_fin_vinculacion': date(2022, 11, 30)
            })
            
    df_vinculacion = pd.DataFrame(vinculaciones)
    print(f"--> Simulados: {len(df_vinculacion)} Vinculaciones (Gente trabajando).")

    # 4. GENERAR GASTOS EJECUTADOS (Tabla Transaccional Financiera)
    # -------------------------------------------------------------
    # Lógica: Cada proyecto generará 20 gastos aleatorios
    gastos_ejec = []
    
    for codigo in df_proyectos['codigo_proyecto']:
        for _ in range(20): # 20 compras por proyecto
            tipo = df_tipo_gasto.sample(1).iloc[0] # Elegir un tipo de gasto al azar
            
            gastos_ejec.append({
                'codigo_proyecto': codigo,
                'id_tipo_gasto': tipo['id_tipo_gasto'],
                'cantidad_unidades': random.randint(1, 10),
                'fecha_registro': date(2022, 6, 15),
                'valor_unitario_historico': tipo['valor_estandar'] # Guardamos el precio del momento
            })
            
    df_gasto_ejecutado = pd.DataFrame(gastos_ejec)
    print(f"--> Simulados: {len(df_gasto_ejecutado)} Movimientos financieros.")

    return df_proyectos, df_tipo_gasto, df_vinculacion, df_gasto_ejecutado