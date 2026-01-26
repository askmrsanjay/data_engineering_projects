from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_medallion_pipeline',
    default_args=default_args,
    description='Orchestrate E-commerce Medallion Pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['ecommerce', 'iceberg'],
) as dag:

    # 1. Check Kafka Connectivity (Diagnostic)
    check_kafka = BashOperator(
        task_id='check_kafka_connectivity',
        bash_command='echo "Checking Kafka..."; sleep 2', # In real scenario, use a connectivity script
    )

    # 3. Trigger Silver Quality Validation (Great Expectations)
    trigger_quality_check = BashOperator(
        task_id='trigger_silver_quality',
        bash_command="""
            docker exec ecommerce-spark-master spark-submit \
            --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 \
            /opt/bitnami/spark/app/src/quality/silver_quality.py
        """,
    )

    # 4. Trigger Gold Layer Aggregations (Batch)
    trigger_gold_job = BashOperator(
        task_id='trigger_gold_batch',
        bash_command="""
            docker exec ecommerce-spark-master spark-submit \
            --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 \
            /opt/bitnami/spark/app/src/batch/gold_batch.py
        """,
    )

    check_kafka >> trigger_silver_job >> trigger_quality_check >> trigger_gold_job
