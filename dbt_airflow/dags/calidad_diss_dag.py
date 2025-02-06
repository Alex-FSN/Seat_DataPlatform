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
    dag_id="calidad_diss_dag",
    default_args=default_args,  # Include default_args here
    start_date=datetime(2024, 1, 1),
    #schedule_interval="@daily",
    schedule_interval="0 6 * * *",
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

DI_SLT_ANFRAGE_job = PythonOperator(
    task_id="DI_SLT_ANFRAGE_JOB",
    python_callable=task_main,
    op_kwargs={
        "Schema": "DISS",
        "Tabla": "DI_SLT_ANFRAGE",
        "year": 2025,
        "month": 1,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

DI_SLT_BEANSTANDUNG_job = PythonOperator(
    task_id="DI_SLT_BEANSTANDUNG",
    python_callable=task_main,
    op_kwargs={
        "Schema": "DISS",
        "Tabla": "DI_SLT_BEANSTANDUNG",
        "year": 2025,
        "month": 1,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

DI_SLT_BILDPOSITION_job = PythonOperator(
    task_id="DI_SLT_BILDPOSITION",
    python_callable=task_main,
    op_kwargs={
        "Schema": "DISS",
        "Tabla": "DI_SLT_BILDPOSITION",
        "year": 2025,
        "month": 1,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

DI_SLT_NACHRICHT_job = PythonOperator(
    task_id="DI_SLT_NACHRICHT",
    python_callable=task_main,
    op_kwargs={
        "Schema": "DISS",
        "Tabla": "DI_SLT_NACHRICHT",
        "year": 2025,
        "month": 1,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

DI_SLT_RANDBEDINGUNG_job = PythonOperator(
    task_id="DI_SLT_RANDBEDINGUNG",
    python_callable=task_main,
    op_kwargs={
        "Schema": "DISS",
        "Tabla": "DI_SLT_RANDBEDINGUNG",
        "year": 2025,
        "month": 1,
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)
dag_delete_folder = PythonOperator(
    task_id="delete_folder",
    op_kwargs={"Schema": "DISS"},
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
        DI_SLT_ANFRAGE_job,
        DI_SLT_BEANSTANDUNG_job,
        DI_SLT_BILDPOSITION_job,
        DI_SLT_NACHRICHT_job,
        DI_SLT_RANDBEDINGUNG_job,
    ]
    >> dag_delete_folder
    >> dummy_task_end
)
