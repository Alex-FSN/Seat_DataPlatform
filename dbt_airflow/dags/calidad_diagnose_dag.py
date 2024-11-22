from airflow.models import DAG
from pendulum import datetime
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from miniocode import func, delete_folder


# Define the DAG function a set of parameters
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    dag_id="calidad_diagnose_dag",
    default_args=default_args,  # Include default_args here
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)


def task_main(**kwargs):
    tabla = kwargs.get("Tabla")
    schema = kwargs.get("Schema")
    year = kwargs.get("year")
    month = kwargs.get("month")
    func(schema, tabla, year, month)


def folder_delete(**kwargs):
    schema = kwargs.get("Schema")
    delete_folder(schema)


dummy_task_start = DummyOperator(
    task_id="start", retries=3, execution_timeout=timedelta(minutes=1)
)  # Set execution timeout)

ODIS_SL_HD_VH_job = PythonOperator(
    task_id="ODIS_SL_HD_VH",
    python_callable=task_main,
    op_kwargs={
        "Schema": "DIAGNOSE",
        "Tabla": "ODIS_SL_HD_VH",
        "year": 2024,
        "month": 11,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

DG_SLT_STEUERGERAET_job = PythonOperator(
    task_id="DG_SLT_STEUERGERAET",
    python_callable=task_main,
    op_kwargs={
        "Schema": "DIAGNOSE",
        "Tabla": "DG_SLT_STEUERGERAET",
        "year": 2024,
        "month": 11,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

# LA ETL NO ESTA DEJANDO DATOS EN MINIO PARA ESTA TABKLA
'''
DG_SLT_PRUEFPLAN_job = PythonOperator(
    task_id="DG_SLT_PRUEFPLAN",
    python_callable=task_main,
    op_kwargs={
        "Schema": "DIAGNOSE",
        "Tabla": "DG_SLT_PRUEFPLAN",
        "year": 2024,
        "month": 11,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)
'''

DG_SLT_PROTOKOLL_job = PythonOperator(
    task_id="DG_SLT_PROTOKOLL",
    python_callable=task_main,
    op_kwargs={
        "Schema": "DIAGNOSE",
        "Tabla": "DG_SLT_PROTOKOLL",
        "year": 2024,
        "month": 11,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

DG_SLT_GLOB_VAR_job = PythonOperator(
    task_id="DG_SLT_GLOB_VAR",
    python_callable=task_main,
    op_kwargs={
        "Schema": "DIAGNOSE",
        "Tabla": "DG_SLT_GLOB_VAR",
        "year": 2024,
        "month": 11,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

DG_SLT_FEHLERSPEICHER_job = PythonOperator(
    task_id="DG_SLT_FEHLERSPEICHER",
    python_callable=task_main,
    op_kwargs={
        "Schema": "DIAGNOSE",
        "Tabla": "DG_SLT_FEHLERSPEICHER",
        "year": 2024,
        "month": 11,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

DG_SLT_ERW_UMW_BEDINGUNGEN_job = PythonOperator(
    task_id="DG_SLT_ERW_UMW_BEDINGUNGEN",
    python_callable=task_main,
    op_kwargs={
        "Schema": "DIAGNOSE",
        "Tabla": "DG_SLT_ERW_UMW_BEDINGUNGEN",
        "year": 2024,
        "month": 11,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

dag_delete_folder = PythonOperator(
    task_id="delete_folder",
    op_kwargs={"Schema": "DIAGNOSE"},
    provide_context=True,
    python_callable=folder_delete,
    dag=dag,
)

dummy_task_end = DummyOperator(
    task_id="end", retries=3, execution_timeout=timedelta(minutes=1)
)  # Set execution timeout)

(
    dummy_task_start
    >> [
        ODIS_SL_HD_VH_job,
        DG_SLT_STEUERGERAET_job,
        DG_SLT_PROTOKOLL_job,
        DG_SLT_GLOB_VAR_job,
        DG_SLT_FEHLERSPEICHER_job,
        DG_SLT_ERW_UMW_BEDINGUNGEN_job
    ]
    >> dag_delete_folder
    >> dummy_task_end
)
