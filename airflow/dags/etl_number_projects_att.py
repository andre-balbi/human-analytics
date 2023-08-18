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

dag = DAG(
    "etl_number_projects_att", default_args=DEFAULT_ARGS, schedule_interval="@once"
)

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

# This code snippet defines a function named "extract" that likely extracts data from a
# database using a SQL query and saves the result to a CSV file in the Staging area.
def extract():
    """Extracts project data from a database using a SQL query and saves the result to a
    CSV file"""

    # Define a SQL query to retrieve the count of projects per employee from the
    # "projects_emp" table.
    query = """SELECT Count(PROJECT_ID) as number_projects
            FROM projects_emp
            GROUP BY (emp_id);"""

    # Execute the SQL query using the provided database engine and load the result into a
    # DataFrame named "df_".
    df_ = pd.read_sql_query(query, engine)

    # Persist the DataFrame into a CSV file in the Staging area with the filename
    # "number_projects.csv".
    df_.to_csv("/tmp/number_projects.csv", index=False)


def load():
    """This code loads data from a CSV file in the Staging area, converts it to the 
    Parquet format, and uploads it to a Data Lake."""

    # Load the data from the Staging area CSV file into a DataFrame named "df_".
    df_ = pd.read_csv("/tmp/number_projects.csv")

    # Convert the data to the Parquet format and save it to a Parquet file.
    df_.to_parquet("/tmp/number_projects.parquet", index=False)

    # Load the Parquet file to the Data Lake using the client's "fput_object" method.
    client.fput_object(
        "processing", "number_projects.parquet", "/tmp/number_projects.parquet"
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
