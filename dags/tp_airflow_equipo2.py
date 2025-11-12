from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# Definir argumentos por defecto del DAG
default_args = {
    'owner': 'equipo2',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Rutas de archivos
BASE_DIR = '/opt/airflow'  # Directorio base dentro del contenedor
INPUT_FILE = os.path.join(BASE_DIR, 'encparticipantes_ep_2024.csv')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'datos_filtrados.csv')

# Función Extract: Leer el archivo CSV
def extract_data(**context):
    """
    Lee el archivo CSV de entrada y guarda los datos en XCom
    """
    print(f"Leyendo datos desde: {INPUT_FILE}")
    datos = pd.read_csv(INPUT_FILE)
    print(f"Datos extraídos: {len(datos)} filas, {len(datos.columns)} columnas")
    
    # Guardar en XCom para pasar a la siguiente tarea
    context['ti'].xcom_push(key='datos_raw', value=datos.to_json())
    return f"Extracción completada: {len(datos)} filas"

# Función Transform: Procesar y filtrar los datos
def transform_data(**context):
    """
    Filtra los datos para obtener solo las filas donde la primera columna es 'Un curso'
    """
    # Obtener datos de la tarea anterior
    ti = context['ti']
    datos_json = ti.xcom_pull(task_ids='extract', key='datos_raw')
    datos = pd.read_json(datos_json)
    
    print(f"Transformando datos. Filas iniciales: {len(datos)}")
    
    # Filtrar filas que tengan 'Un curso' en la primera columna
    columna_cursos = datos.columns[0]
    datos_filtrados = datos[datos[columna_cursos] == 'Un curso']
    
    print(f"Datos filtrados: {len(datos_filtrados)} filas")
    
    # Guardar en XCom para la siguiente tarea
    context['ti'].xcom_push(key='datos_transformados', value=datos_filtrados.to_json())
    return f"Transformación completada: {len(datos_filtrados)} filas filtradas"

# Función Load: Guardar los datos procesados
def load_data(**context):
    """
    Guarda los datos procesados en un archivo CSV en la carpeta output
    """
    # Obtener datos de la tarea anterior
    ti = context['ti']
    datos_json = ti.xcom_pull(task_ids='transform', key='datos_transformados')
    datos_filtrados = pd.read_json(datos_json)
    
    # Crear carpeta output si no existe
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Guardar el DataFrame filtrado
    datos_filtrados.to_csv(OUTPUT_FILE, index=False)
    print(f"Datos guardados en: {OUTPUT_FILE}")
    print(f"Total de filas guardadas: {len(datos_filtrados)}")
    
    return f"Carga completada: archivo guardado en {OUTPUT_FILE}"

# Definir el DAG
with DAG(
    dag_id='tp_airflow_equipo2',
    default_args=default_args,
    description='DAG ETL para procesar datos de encuesta de participantes',
    schedule=None,  # Se ejecuta manualmente (antes era schedule_interval en Airflow 2.x)
    start_date=datetime(2025, 11, 12),
    catchup=False,
    tags=['equipo2', 'etl', 'tp'],
) as dag:
    
    # Tarea 1: Extract
    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )
    
    # Tarea 2: Transform
    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
    )
    
    # Tarea 3: Load
    task_load = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )
    
    # Definir el orden de ejecución: Extract -> Transform -> Load
    task_extract >> task_transform >> task_load
