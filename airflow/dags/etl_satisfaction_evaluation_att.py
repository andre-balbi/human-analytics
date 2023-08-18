from datetime import date, datetime, timedelta

import pandas as pd
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from minio import Minio

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 13),
}

dag = DAG(
    "etl_satisfaction_evaluation_att",
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
)

data_lake_server = Variable.get("data_lake_server")
data_lake_login = Variable.get("data_lake_login")
data_lake_password = Variable.get("data_lake_password")

client = Minio(
    data_lake_server,
    access_key=data_lake_login,
    secret_key=data_lake_password,
    secure=False,
)


def extract():

    """Extracts data from a JSON file in the Data Lake and saves the result to a JSON
    file in the Staging area."""

    # Extract data from the Data Lake using the client's "get_object" method.
    obj = client.get_object(
        "landing",
        "performance-evaluation/employee_performance_evaluation.json",
    )
    data = obj.read()

    # Load the extracted JSON data into a DataFrame named "df_".
    df_ = pd.read_json(data, lines=True)

    # Persist the DataFrame into a JSON file in the Staging area with the filename
    # "employee_performance_evaluation.json".
    df_.to_json(
        "/tmp/employee_performance_evaluation.json",  # File path and name
        orient="records",  # Format the JSON as records (one record per line)
        lines=True,  # Each line is a separate JSON
    )


def load():

    """Loads data from a JSON file in the Staging area, extracts specific columns,
    converts it to the Parquet format, and uploads it to a Data Lake."""

    from io import BytesIO

    # Load the data from the Staging area JSON file into a DataFrame named "df_".
    df_ = pd.read_json(
        "/tmp/employee_performance_evaluation.json", orient="records", lines="True"
    )

    # Extract specific columns and convert the data to the Parquet format.
    df_[["satisfaction_level", "last_evaluation"]].to_parquet(
        "/tmp/satisfaction_evaluation.parquet", index=False
    )

    # Load the Parquet file to the Data Lake using the client's "fput_object" method.
    client.fput_object(
        "processing",  # Destination where the file will be saved
        "satisfaction_evaluation.parquet",  # Name of the file
        "/tmp/satisfaction_evaluation.parquet",  # File address
    )


extract_task = PythonOperator(
    task_id="extract_file_from_data_lake",
    provide_context=True,
    python_callable=extract,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_file_to_data_lake",
    provide_context=True,
    python_callable=load,
    dag=dag,
)

clean_task = BashOperator(
    task_id="clean_files_on_staging",
    bash_command="rm -f /tmp/*.csv;rm -f /tmp/*.json;rm -f /tmp/*.parquet;",
    dag=dag,
)

extract_task >> load_task >> clean_task
