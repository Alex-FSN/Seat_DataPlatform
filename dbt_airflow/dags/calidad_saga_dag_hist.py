from airflow.models import DAG
from pendulum import datetime
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from miniocode import func, delete_folder, func_hist


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
    dag_id="calidad_saga_dag_hist",
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
    func_hist(schema, tabla, year, month)


def folder_delete(**kwargs):
    schema = kwargs.get("Schema")
    delete_folder(schema)


dummy_task_start = DummyOperator(
    task_id="start", retries=3, execution_timeout=timedelta(minutes=10)
)  # Set execution timeout)


SA_SLT_ANTRSCHADEN_job = PythonOperator(
    task_id="SA_SLT_ANTRSCHADEN",
    python_callable=task_main,
    op_kwargs={
        "Schema": "SAGA",
        "Tabla": "SA_SLT_ANTRSCHADEN",
        "year": 2024,
        "month": 11,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

SA_SLT_ANTRSCHADEN_APETFLFM_job = PythonOperator(
    task_id="SA_SLT_ANTRSCHADEN_APETFLFM",
    python_callable=task_main,
    op_kwargs={
        "Schema": "SAGA",
        "Tabla": "SA_SLT_ANTRSCHADEN_APETFLFM",
        "year": 2024,
        "month": 11,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

dag_delete_folder = PythonOperator(
    task_id="delete_folder",
    op_kwargs={"Schema": "SAGA"},
    provide_context=True,
    python_callable=folder_delete,
    dag=dag,
)

dummy_task_end = DummyOperator(
    task_id="end", retries=3, execution_timeout=timedelta(minutes=10)
)  # Set execution timeout)

(
    dummy_task_start
    >> [SA_SLT_ANTRSCHADEN_job, SA_SLT_ANTRSCHADEN_APETFLFM_job]
    >> dag_delete_folder
    >> dummy_task_end
)
