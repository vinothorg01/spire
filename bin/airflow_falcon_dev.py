# import airflow
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksRunNowOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
import pendulum

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
    dag_id="Falcon_{brand_name}",  # TODO update the brand name here
    default_args=default_args,
    schedule_interval="*/60 * * * *",
    catchup=False,
)

notebook_params = {"Brand_Name": "self", "Mode": "dev"}

# t1, t2 and t3 are examples of tasks created by instantiating operators

job_id = 111

notebook_task = DatabricksRunNowOperator(
    task_id="notebook_task", job_id=job_id, notebook_params=notebook_params, dag=dag
)

t2 = SSHOperator(
    ssh_conn_id="falcon_prod",
    task_id="run_socialflow_data_job",
    command="/home/akatiyal/datasci-virality/bin/airflow-dev/brand={}/airflow_socialposted_data.sh ",  # TODO update the brand name
    dag=dag,
)

t3 = SSHOperator(
    ssh_conn_id="falcon_prod",
    task_id="run_predictions",
    command="/home/akatiyal/datasci-virality/bin/airflow-dev/brand={}/airflow_run.sh ",  # TODO update the brand name
    dag=dag,
)

t4 = SSHOperator(
    ssh_conn_id="falcon_prod",
    task_id="run_logging_data_job",
    command="/home/akatiyal/datasci-virality/bin/airflow-dev/brand={}/airflow_logging.sh ",  # TODO update the brand name
    dag=dag,
)

t2.set_downstream(notebook_task)
notebook_task.set_downstream(t3)
t3.set_downstream(t4)
