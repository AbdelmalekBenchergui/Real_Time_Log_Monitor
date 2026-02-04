from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="logs_streaming_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,   
    catchup=False,
    tags=["kafka", "spark", "streaming"]
) as dag:

    start_producer = BashOperator(
        task_id="start_kafka_producer",
        bash_command='python3 /opt/airflow/src/producer.py',
    )

    start_consumer = BashOperator(
        task_id="start_spark_streaming",
        bash_command="""
        spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,\
org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0 \
        /opt/airflow/src/consumer.py
        """
    )



[start_producer, start_consumer] 
