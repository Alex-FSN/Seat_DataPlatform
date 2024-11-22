from minio import Minio
from minio.error import S3Error
from urllib3 import Retry
from datetime import datetime, date
from time import sleep
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from utils.utils import execute_query_by_name
from dotenv import load_dotenv
import urllib3
import socket
import os
import shutil
import requests

load_dotenv()

# Desactivar las advertencias de verificación de SSL
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

dbt_project_path = "/usr/local/airflow/dags/"

def delete_folder(schema):
    if os.path.isdir(dbt_project_path + schema):
        shutil.rmtree(dbt_project_path + schema)
    elif os.path.isfile(dbt_project_path + schema):
        os.remove(dbt_project_path + schema)
    else:
        print(f"The path {dbt_project_path+schema} does not exist.")


def devolver_year(name):
    partes = name.split("/")
    # Obtener la parte que contiene el año (DAPC_YEAR=2024)
    parte_con_year = partes[2]

    # Dividir esa parte en función del signo igual (=)
    valor_year = parte_con_year.split("=")

    # Obtener el año y convertirlo a entero
    year_string = valor_year[1]
    year_obj = 0

    # Verificar si el año contiene solo dígitos
    if year_string.isdigit():
        year_obj = int(year_string)

        print("Año:", year_obj)
    else:
        print("El valor del año no es un número válido.")
    return year_obj


def devolver_mes(name):
    partes = name.split("/")
    # Obtener la parte que contiene el año (DAPC_YEAR=2024)
    parte_con_mes = partes[3]

    # Dividir esa parte en función del signo igual (=)
    valor_mes = parte_con_mes.split("=")

    # Obtener el año y convertirlo a entero
    mes_string = valor_mes[1]
    mes_obj = 0

    # Verificar si el año contiene solo dígitos
    if mes_string.isdigit():
        mes_obj = int(mes_string)

        print("Mes:", mes_obj)
    else:
        print("El valor del año no es un número válido.")
    return mes_obj


def snowflake_con(schema):
    # Create the SQLAlchemy engine
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_id")
    engine = dwh_hook.get_cursor()

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


def insert_to_sourcetable(name, conn):
    partes = name.split("/")
    table = partes[1]
    schema = partes[0]
    fecha = date.today()
    add = {"table_name": table, "fecha": fecha, "name_file": name, "schema": schema}
    execute_query_by_name("inserdata", add, conn, "minicode.sql")


def insert_to_snowflake(tabla, file_path, schema, engine, firsload):
    print(file_path)
    partes = file_path.split("/")
    size = len(partes)
    name_parquet = partes[size - 1]

    print(name_parquet)
    database = "PRO_BRONZE_DB"
    stage = f"{database}.{schema}.{tabla}"
    taux_tabla = "taux_" + str(datetime.today().strftime("%Y%m%d")) + "_" + tabla

    arguments = {
        "stage_name": stage,
        "schema": schema,
        "nametable": tabla,
        "name": file_path,
        "name_parquet": name_parquet,
        "taux_nametable": taux_tabla,
    }

    val = execute_query_by_name("existstage", arguments, engine, "minicode.sql")
    if int(val) == 0:
        execute_query_by_name("createstage", arguments, engine, "minicode.sql")
    execute_query_by_name("addfilestage", arguments, engine, "minicode.sql")
    # execute_query_by_name('createformat',arguments,engine)
    if firsload == 1:
        execute_query_by_name("createtable", arguments, engine, "minicode.sql")
        execute_query_by_name("add_insertdate", arguments, engine, "minicode.sql")

    execute_query_by_name("createtabletaux", arguments, engine, "minicode.sql")

    execute_query_by_name("copytotable", arguments, engine, "minicode.sql")
    execute_query_by_name("insert_to_finall_table", arguments, engine, "minicode.sql")
    execute_query_by_name("remove_taux_table", arguments, engine, "minicode.sql")

    # execute_query_by_name('removestage',arguments,engine)


def processwritesnowflake(object_name, client, bucket_name, engine, tabla, firsload):
    file_path = (
        dbt_project_path + object_name
    )  # Ruta local donde se guardará el archivo
    client.fget_object(bucket_name, object_name, file_path)
    print(f"Archivo descargado: {object_name}")
    # transformar parquet a dataframe
    # inserta source table control
    # insertar datos a snowflake
    partes = object_name.split("/")
    insert_to_snowflake(tabla, file_path, partes[0], engine, firsload)
    insert_to_sourcetable(object_name, engine)
    # eliminar archivo
    sleep(10)
    if os.path.exists(dbt_project_path + object_name):
        os.remove(dbt_project_path + object_name)
    else:
        print(f"File or directory {dbt_project_path+object_name} does not exist.")


# Funcion principal codigo
def func(schema, tabla, year, month):
    acces_key = os.getenv("acces_key")
    secret_key = os.getenv("secret_key")
    minio_url = os.getenv("minio_url")

    acces_key = "EsqaS3OZCSEAT"
    secret_key = "bxe3ymy_pwq1CHP7fbm"
    minio_url = "storage-ui.esqa.dapc.ocp.vwgroup.com:443"
    hostname = minio_url

    # procerso principal para la transformaciond de parquet a pandas para escribir hacía snowflake
    if not acces_key or not secret_key or not minio_url:
        raise ValueError(
            "Error al leer access_key o secret_key de acceso a minio"
        )

    # conexion con minIO
    try:
        ip_address = socket.gethostbyname(hostname)
        print(f"The IP address   {hostname} is {ip_address}")
    except socket.gaierror as e:
        print(f"Error: {e.strerror}")
   
    httpclient = urllib3.PoolManager(
        timeout=urllib3.Timeout(connect=10.0, read=10.0),
        cert_reqs="CERT_NONE",
        maxsize=5,
        retries=Retry(
            total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504]
        ),
    )
    
    """httpclient2 = urllib3.ProxyManager(
                    'https://proxy_host.sampledomain.com:8119/',
                    cert_reqs='CERT_NONE',
                    ca_certs='CA-Bundle.crt',
                    timeout=urllib3.Timeout(connect=10.0, read=10.0)
                )"""
    '''
    client = Minio(
        minio_url,
        access_key=acces_key,
        secret_key=secret_key,
        secure=True,
        http_client=httpclient,
    )
    '''
    
    client = Minio("storage.esqa.dapc.ocp.vwgroup.com", 
        access_key="EsqaS3OZCSEAT",
        secret_key="bxe3ymy_pwq1CHP7fbm",
        secure=True,
        http_client=httpclient
    )
    
    bucket_name = "sqa-sz-storage"
    # definicion del schema hacia donde se va escribir
    prefix = schema
    recursive = True
    # procese de descarga de los parquet y su tratamiento
    try:
        if (
            (schema == "STAMMDATEN")
            or tabla == "CA_SLT_FAHRZEUG_FILTER"
            and schema == "CARPORT"
        ):
            objects = client.list_objects(
                bucket_name, prefix=prefix + "/" + tabla, recursive=recursive
            )
        else:
            objects = client.list_objects(
                bucket_name,
                prefix=prefix + "/" + tabla + "/DAPC_YEAR=" + str(year)
                + "/DAPC_MONTH=" + str(month),   
                recursive=recursive,
            )
        #comentar / descomentar  "/DAPC_MONTH=" + str(month) linea anterior para cgargas por mes o año y mes 

        engine = snowflake_con("your_schema")
        
        filestart = schema + "/" + tabla

        parametros = {"schema": schema, "nametable": tabla}
        execute_query_by_name("defaultdatabase", parametros, engine, "minicode.sql")

        # execute_query_by_name('createschema', parametros,engine)
        execute_query_by_name("createcontrol", parametros, engine, "minicode.sql")

        # execute_query_by_name('createformat', parametros,engine)
        # Primera carga all datat
        if (
            int(execute_query_by_name("firstload", parametros, engine, "minicode.sql"))
            == 0
        ):
            for obj in objects:
                name = obj.object_name
                print(name)
                if (
                    (obj.object_name).endswith(".parquet")
                    and (
                        ((obj.object_name).startswith(filestart + "/DAPC_LADEDATUM"))
                        or ((obj.object_name).startswith(filestart + "/folder_"))
                        or (
                            (obj.object_name).startswith(filestart + "/DAPC_YEAR")
                            and devolver_year(name) == year
                            and devolver_mes(name) == month  #descomentar si se quiere hacer carga por año y mes
                        )
                    )
                    and not (obj.object_name).endswith("checkpoint.parquet")
                ):
                    object_name = obj.object_name
                    processwritesnowflake(
                        object_name, client, bucket_name, engine, tabla, 1
                    )
        # Carga incremental
        else:
            for obj in objects:
                name = obj.object_name
                print(name)
                if (
                    (obj.object_name).endswith(".parquet")
                    and (
                        ((obj.object_name).startswith(filestart + "/DAPC_LADEDATUM"))
                        or ((obj.object_name).startswith(filestart + "/folder_"))
                        or (
                            (obj.object_name).startswith(filestart + "/DAPC_YEAR")
                            and devolver_year(name) == year
                            and devolver_mes(name) == month  #comentar / descomentar si se quiere hacer carga por año y mes
                        )
                    )
                    and not (obj.object_name).endswith("checkpoint.parquet")
                ):
                    parametros = {"nametable": obj.object_name, "schema": schema}
                    val = execute_query_by_name(
                        "existeparquet", parametros, engine, "minicode.sql"
                    )
                    print(val)
                    if int(val) == 0:
                        object_name = obj.object_name
                        processwritesnowflake(
                            object_name, client, bucket_name, engine, tabla, 0
                        )

                    else:
                        continue
        engine.close()

    except S3Error as e:
        print(f"Error listing objects: {e}")
