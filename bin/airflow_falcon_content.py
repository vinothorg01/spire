# import airflow
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.bash_operator import BashOperator
import pendulum
from airflow.contrib.operators.ssh_operator import SSHOperator

# from datetime import datetime,timedelta

tz = pendulum.timezone("US/Eastern")

default_args = {
    "owner": "Anuj Katiyal",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 5, 1, tzinfo=tz),
    "email": ["anuj_katiyal@condenast.com"],
    "email_on_failure": True,
}

dag = DAG(
    dag_id="Falcon_Content_Creation_{}",  # TODO: update brand name
    default_args=default_args,
    schedule_interval="45 * * * *",
    catchup=False,
)

# t1, t2 and t3 are examples of tasks created by instantiating operators

t1 = SSHOperator(
    ssh_conn_id="falcon_prod",
    task_id="run_content_data_job",
    command="/home/akatiyal/datasci-virality/bin/airflow/brand={}/airflow_content.sh ",  # TODO: update brand
    dag=dag,
)
