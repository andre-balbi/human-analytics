import math
from datetime import date, datetime, timedelta
from io import BytesIO

import pandas as pd
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from minio import Minio
from sqlalchemy.engine import create_engine

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 13),
}

dag = DAG("etl_work_accident_att", default_args=DEFAULT_ARGS, schedule_interval="@once")

data_lake_server = Variable.get("data_lake_server")
data_lake_login = Variable.get("data_lake_login")
data_lake_password = Variable.get("data_lake_password")

database_server = Variable.get("database_server")
database_login = Variable.get("database_login")
database_password = Variable.get("database_password")
database_name = Variable.get("database_name")


url_connection = "mysql+pymysql://{}:{}@{}/{}".format(
    str(database_login),
    str(database_password),
    str(database_server),
    str(database_name),
)

engine = create_engine(url_connection)

client = Minio(
    data_lake_server,
    access_key=data_lake_login,
    secret_key=data_lake_password,
    secure=False,
)


def extract():

    """Extracts accident-related data for employees from database tables and saves it to
    a CSV file in the Staging area."""

    # Obtain the "employees" table data and store it as a DataFrame named "df_employees".
    df_employees = pd.read_sql_table("employees", engine)

    # Obtain the "accident" table data and store it as a DataFrame named "df_accident".
    df_accident = pd.read_sql_table("accident", engine)

    # Initialize an empty list to store work accident information for employees.
    work_accident = []
    # Iterate through each employee in the "df_employees" DataFrame
    for emp in df_employees["emp_no"]:
        if emp in df_accident["emp_no"].to_list():
            work_accident.append(1)
        else:
            work_accident.append(0)

    # Create the structure of a temporary DataFrame named "df_" and assign the work
    # accident data to it.
    df_ = pd.DataFrame(data=None, columns=["work_accident"])
    df_["work_accident"] = work_accident

    # Persist the DataFrame into a CSV file in the Staging area with the filename
    # "work_accident.csv".
    df_.to_csv("/tmp/work_accident.csv", index=False)


def load():

    """Loads work accident data from a CSV file, converts to Parquet format, and
    uploads to a Data Lake."""

    # Load the work accident data from the Staging area CSV file into a DataFrame named "df_".
    df_ = pd.read_csv("/tmp/work_accident.csv")

    # Convert the data to the Parquet format and save it to a Parquet file.
    df_.to_parquet("/tmp/work_accident.parquet", index=False)

    # Load the Parquet file to the Data Lake using the client's "fput_object" method.
    client.fput_object(
        "processing",  # Destination where the file will be saved
        "work_accident.parquet",  # Name of the file
        "/tmp/work_accident.parquet",  # File address
    )


extract_task = PythonOperator(
    task_id="extract_data_from_database",
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
