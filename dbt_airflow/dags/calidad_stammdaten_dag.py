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
    dag_id="calidad_STAMMDATEN_dag",
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
    task_id="start", retries=3, execution_timeout=timedelta(minutes=10)
)  # Set execution timeout)


SQ_SLT_KEFA_FEHLEROBJEKT_job = PythonOperator(
    task_id="SQ_SLT_KEFA_FEHLEROBJEKT",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_KEFA_FEHLEROBJEKT",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

SQ_SLT_KEFA_FEHLERART_job = PythonOperator(
    task_id="SQ_SLT_KEFA_FEHLERART",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_KEFA_FEHLERART",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

SQ_SLT_KEFA_LAGE_job = PythonOperator(
    task_id="SQ_SLT_KEFA_LAGE",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_KEFA_LAGE",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

SQ_SLT_KEFA_MATRIX_FO_job = PythonOperator(
    task_id="SQ_SLT_KEFA_MATRIX_FO",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_KEFA_MATRIX_FO",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

SQ_SLT_KEFA_POSITION_job = PythonOperator(
    task_id="SQ_SLT_KEFA_POSITION",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_KEFA_POSITION",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)


SQ_SLT_KEFA_RANDBEDINGUNG_job = PythonOperator(
    task_id="SQ_SLT_KEFA_RANDBEDINGUNG",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_KEFA_RANDBEDINGUNG",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

SQ_SLT_KEFA_VERWENDUNG_job = PythonOperator(
    task_id="SQ_SLT_KEFA_VERWENDUNG",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_KEFA_VERWENDUNG",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

SQ_SLT_KDNR_KDNRGRUPPE_job = PythonOperator(
    task_id="SQ_SLT_KDNR_KDNRGRUPPE",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_KDNR_KDNRGRUPPE",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

SQ_SLT_KDNR_job = PythonOperator(
    task_id="SQ_SLT_KDNR",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_KDNR",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)


SQ_SLT_KDNRGRUPPEN_job = PythonOperator(
    task_id="SQ_SLT_KDNRGRUPPEN",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_KDNRGRUPPEN",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

SQ_SLT_DG_STGERAET_TEXT_job = PythonOperator(
    task_id="SQ_SLT_DG_STGERAET_TEXT",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_DG_STGERAET_TEXT",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

SQ_SLT_GETRIEBE_job = PythonOperator(
    task_id="SQ_SLT_GETRIEBE",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_GETRIEBE",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

SQ_SLT_ISOLAND_job = PythonOperator(
    task_id="SQ_SLT_ISOLAND",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_ISOLAND",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)


SQ_SLT_MOTORT_job = PythonOperator(
    task_id="SQ_SLT_MOTOR",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_MOTOR",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)


SQ_SLT_PARTNER_job = PythonOperator(
    task_id="SQ_SLT_PARTNER",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_PARTNER",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)


SQ_SLT_PRNR_job = PythonOperator(
    task_id="SQ_SLT_PRNR",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_PRNR",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)


SQ_SLT_VMODELL_job = PythonOperator(
    task_id="SQ_SLT_VMODELL",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_VMODELL",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)

SQ_SLT_WERK_job = PythonOperator(
    task_id="SQ_SLT_WERK",
    python_callable=task_main,
    op_kwargs={
        "Schema": "STAMMDATEN",
        "Tabla": "SQ_SLT_WERK",
    },  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)


dag_delete_folder = PythonOperator(
    task_id="delete_folder",
    op_kwargs={"Schema": "STAMMDATEN"},
    provide_context=True,
    python_callable=folder_delete,
    dag=dag,
)

dummy_task_end = DummyOperator(
    task_id="end", retries=3, execution_timeout=timedelta(minutes=10)
)  # Set execution timeout)

(

    dummy_task_start
    >> [
        SQ_SLT_KEFA_FEHLEROBJEKT_job,
        SQ_SLT_KEFA_FEHLERART_job,
        SQ_SLT_KEFA_LAGE_job,
        SQ_SLT_KEFA_MATRIX_FO_job,
        SQ_SLT_KEFA_POSITION_job,
        SQ_SLT_KEFA_RANDBEDINGUNG_job,
        SQ_SLT_KEFA_VERWENDUNG_job,
        SQ_SLT_KDNR_KDNRGRUPPE_job,
        SQ_SLT_KDNR_job,
        SQ_SLT_KDNRGRUPPEN_job,
        SQ_SLT_DG_STGERAET_TEXT_job,
        SQ_SLT_GETRIEBE_job,
        SQ_SLT_ISOLAND_job,
        SQ_SLT_MOTORT_job,
        SQ_SLT_PARTNER_job,
        SQ_SLT_PRNR_job,
        SQ_SLT_VMODELL_job,
        SQ_SLT_WERK_job
    ]
    >> dag_delete_folder
    >> dummy_task_end

)
