
import urllib3
import shutil
import pandas as pd
from dotenv import load_dotenv
from datetime import date
from pathlib import Path
from sqlalchemy import create_engine, text
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
import os, re,unicodedata
from azure.storage.blob import BlobServiceClient, BlobClient



# Variables locales 
dbt_project_path = ("/usr/local/airflow/dags/")
load_dotenv()
account_url = os.getenv("account_url")
sas_token= os.getenv("sas_token")
file_path = dbt_project_path+"descargas_csv"  # Ruta local donde se guardará el archivo


def snowflake_con():
   
    # Create the SQLAlchemy engine
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_id")
    engine= dwh_hook.get_cursor()
    
    with dwh_hook.get_conn() as connection:
        with connection.cursor() as cursor:
        # Ejecuta la consulta
            cursor.execute("SELECT CURRENT_VERSION()")
        # Obtén los resultados
        rows = cursor.fetchall()
        # Procesa los resultados (por ejemplo, imprime el primer resultado)
        if rows:
            print(rows[0][0])
        else:
            print("No se encontraron resultados.")
    return engine
    

def remove_accents(input_str):
    """Elimina los acentos de una cadena de texto."""
    # Normaliza la cadena para descomponer los caracteres acentuados en su forma base y los acentos
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    # Filtra solo los caracteres que no son marcas diacríticas
    return ''.join(c for c in nfkd_form if not unicodedata.combining(c))

#Detectar automaticamente el delimitador del csv 
def detect_delimiter(file_path):

    with open(file_path, 'r') as file:
        first_line = file.readline()
        # Asume que los delimitadores posibles son ',', ';', '\t'
        if ',' in first_line:
            return ','
        elif ';' in first_line:
            return ';'
        elif '\t' in first_line:
            return '\t'
        else:
            raise ValueError("Delimiter not found")

#executa las queries que estan en rvscode.sql
def execute_query_by_name(query_name, params ,conn):
    # Lee el archivo SQL
    file_name = dbt_project_path+'rvscode.sql'
    if params is None:
        params = {}

    with open(file_name, 'r') as file:
        contenido = file.read()
        consultas = contenido.split('--')
        consulta_sql = ''
        for consulta in consultas:
            if consulta.strip().startswith(f"@{query_name}"):
                consulta_sql = consulta.strip().split("\n", 1)[1].strip()
                break
        else:
            raise ValueError("No se encontró la consulta especificada en el archivo")
    #executa la query que se manda
    consulta_formateada = consulta_sql.format(**params)
    print(consulta_formateada)
    result=conn.execute(consulta_formateada)
    rows = result.fetchall()
    print((rows[0][0]))
    return (rows[0][0])


#Descarga el csv y lo inserta a snowflake en forma de tabla 
def download_blob_to_file(conn):
    blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
    container_client = blob_service_client.get_container_client('hvr-prueba-csv')
    print(container_client)
    download_folder=dbt_project_path+'descargas_csv'
    os.makedirs(download_folder, exist_ok=True)

    # Listar y descargar los archivos CSV
    blobs = container_client.list_blobs()
    print(blobs)

    for blob in blobs:
        if blob.name.endswith('.csv'):
            # Crear un BlobClient
            blob_client = container_client.get_blob_client(blob)
            blob_name = remove_accents(blob.name)
            # Ruta completa para guardar el archivo descargado
            download_file_path = os.path.join(download_folder, blob_name)
            
            # Descargar el blob
            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())

            #Si un fichero viene mal formateado le hacemos un arreo
            name_table = re.sub(r'[^\w]', '_',  os.path.splitext(blob_name)[0])
            print(name_table)

            print(f"Archivo descargado: {download_file_path}")
            delimiter= detect_delimiter(file_path+'/'+blob_name)
            path_route = download_folder+"/"+blob_name
            print(delimiter)
            execute_query_by_name('databasedefintion',None,conn)
            execute_query_by_name('createstage',None,conn)
            parametros = {'path_route' : path_route , 'stage_name':'rvs_table.RVS_FILE_CSV','name_csv':blob_name,'name_table': name_table,'delimiter': delimiter}
            execute_query_by_name('addfilestage',parametros,conn)
            execute_query_by_name('fileformat',parametros,conn)
            execute_query_by_name('createtable',parametros,conn)
            execute_query_by_name('copyinto',parametros,conn)

#función principal para que funcione todo el proceso
def createstage():
    conn = snowflake_con()
    download_blob_to_file(conn)

def delete_folder():
    folder=dbt_project_path+'descargas_csv'
    if os.path.isdir(folder):
        shutil.rmtree(folder)
    elif os.path.isfile(folder):
        os.remove(folder)
    else:
        print(f"The path {folder} does not exist.")
     

