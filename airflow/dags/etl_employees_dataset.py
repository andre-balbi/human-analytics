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

dag = DAG("etl_employees_dataset", default_args=DEFAULT_ARGS, schedule_interval="@once")

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
    """Extracts data from Parquet files in the data lake, combines them into a DataFrame,
    and saves as a CSV file"""

    # Create an empty DataFrame to store extracted data.
    df = pd.DataFrame(data=None)

    # Retrieve a list of objects from the "processing" bucket in the data lake.
    objects = client.list_objects("processing", recursive=True)

    # Iterate through each object in the list.
    for obj in objects:
        # Display message indicating file download and print object details.
        print("Downloading file...")
        print(obj.bucket_name, obj.object_name.encode("utf-8"))

        # Download the Parquet file and save it as "/tmp/temp_.parquet".
        client.fget_object(
            obj.bucket_name,
            obj.object_name.encode("utf-8"),
            "/tmp/temp_.parquet",
        )

        # Read the downloaded Parquet file into a temporary DataFrame.
        df_temp = pd.read_parquet("/tmp/temp_.parquet")

        # Concatenate the temporary DataFrame horizontally with the main DataFrame.
        df = pd.concat([df, df_temp], axis=1)

    # Save the combined DataFrame as a CSV file in the staging area.
    df.to_csv("/tmp/employees_dataset.csv", index=False)


def load():
    """Transforms and loads the processed employee dataset into Parquet format and uploads
    it to the Data Lake."""

    # Load the processed employee dataset from the staging area.
    df_ = pd.read_csv("/tmp/employees_dataset.csv")

    # Convert the data to the Parquet format and save it to a Parquet file.
    df_.to_parquet("/tmp/employees_dataset.parquet", index=False)

    # Upload the Parquet file to the "processing" bucket in the Data Lake.
    client.fput_object(
        "processing", "employees_dataset.parquet", "/tmp/employees_dataset.parquet"
    )


extract_task = PythonOperator(
    task_id="extract_data_from_datalake",
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
