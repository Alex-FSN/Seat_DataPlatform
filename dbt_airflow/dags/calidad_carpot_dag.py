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
    dag_id="calidad_carpot_dag",
    default_args=default_args,  # Include default_args here
    start_date=datetime(2022, 1, 1),
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

CA_SLT_FAHRZEUG_JOB = PythonOperator(
    task_id="CA_SLT_FAHRZEUG",
    python_callable=task_main,
    op_kwargs={
        "Schema": "CARPORT",
        "Tabla": "CA_SLT_FAHRZEUG",
        "year": 2022,
        "month": 10,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

CA_SLT_FAHRZEUG_FILTER_JOB = PythonOperator(
    task_id="CA_SLT_FAHRZEUG_FILTER",
    python_callable=task_main,
    op_kwargs={
        "Schema": "CARPORT",
        "Tabla": "CA_SLT_FAHRZEUG_FILTER",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

CA_SLT_FAHRZEUG_PRNR_JOB = PythonOperator(
    task_id="CA_SLT_FAHRZEUG_PRNR",
    python_callable=task_main,
    op_kwargs={
        "Schema": "CARPORT",
        "Tabla": "CA_SLT_FAHRZEUG_PRNR",
        "year": 2022,
        "month": 10,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

CA_SLT_FAHRZEUG_PRNR_STRING_JOB = PythonOperator(
    task_id="CA_SLT_FAHRZEUG_PRNR_STRING",
    python_callable=task_main,
    op_kwargs={
        "Schema": "CARPORT",
        "Tabla": "CA_SLT_FAHRZEUG_PRNR_STRING",
        "year": 2022,
        "month": 10,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

dag_delete_folder = PythonOperator(
    task_id="delete_folder",
    op_kwargs={"Schema": "CARPORT"},
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
        CA_SLT_FAHRZEUG_JOB,
        CA_SLT_FAHRZEUG_FILTER_JOB,
        CA_SLT_FAHRZEUG_PRNR_JOB,
        CA_SLT_FAHRZEUG_PRNR_STRING_JOB,
    ]
    >> dag_delete_folder
    >> dummy_task_end
)