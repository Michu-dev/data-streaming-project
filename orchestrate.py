from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "project-workflow",
    start_date=datetime(2015, 12, 1),
    schedule_interval=None,
    # TODO Running the solution each time in the configuration of Apache Airflow script correct the paths in parameters jars_home and bucket_home
    params={
      "username": Param("<username_gcp>", type="string"),
      "cluster_name": Param("<cluster-name>", type="string"),
      "delay": Param("A", enum=["A", "C"]),
      "time_length": Param(None, type=["null", "string"]),
      "ratings_number": Param(None, type=["null", "string"]),
      "avg_rating": Param(None, type=["null", "string"]),
    },
    render_template_as_native_obj=True
) as dag:
    kafka_producer_setup = BashOperator(
        task_id="kafka_producer_setup",
        bash_command="""chmod 744 /home/{{ params.username }}/kafka_producer_setup.sh && bash /home/{{ params.username }}/kafka_producer_setup.sh"""
    )
    sink_preparation = BashOperator(
        task_id="sink_preparation",
        bash_command="""chmod 744 /home/{{ params.username }}/sink_commands.sh && bash /home/{{ params.username }}/sink_commands.sh"""
    )
    etl_streaming_job = BashOperator(
        task_id="etl_job",
        bash_command="""./bin/spark-submit \
                            --verbose
                            --master yarn \
                            --deploy-mode cluster \
                            --driver-memory 8g \
                            --executor-memory 16g \
                            --executor-cores 2  \
                            /home/{{ params.username }}/etl_jobs.py {{ params.cluster_name }} {{ params.delay }} {{ params.time_length }} {{ params.ratings_number }} {{ params.avg_rating }}"""
    )
    sink_cleanup = BashOperator(
        task_id="sink_cleanup",
        bash_command="psql -h localhost -p 8432 -U postgres -d postgres -f /home/{{ params.username }}/ddl_cleanup.sql"
    )

kafka_producer_setup >> sink_preparation
sink_preparation >> etl_streaming_job