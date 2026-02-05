from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
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
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['ecommerce', 'iceberg'],
) as dag:

    # 1. Trigger Silver Batch
    trigger_silver = DockerOperator(
        task_id='trigger_silver_batch',
        image='bitnami/spark:3',
        container_name='airflow_spark_submit_silver',
        api_version='auto',
        auto_remove=True,
        command="spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 /opt/bitnami/spark/app/src/batch/silver_batch.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="ecommerce_docker_default",
        mounts=[Mount(source="/d/Projects/data_engineering_projects/ecommerce", target="/opt/bitnami/spark/app", type="bind")],
        environment={
            'SPARK_MODE': 'worker',
            'SPARK_MASTER_URL': 'spark://ecommerce-spark-master:7077'
        }
    )

    # 2. Key Step: Great Expectations (Optional for now, but good to have)
    # We will trigger the same container image but run the quality script
    trigger_quality = DockerOperator(
        task_id='trigger_quality_check',
        image='bitnami/spark:3',
        container_name='airflow_spark_submit_quality',
        api_version='auto',
        auto_remove=True,
        command='bash -c "export HOME=/tmp && pip install --user great_expectations==0.18.3 && export PYTHONPATH=$PYTHONPATH:/tmp/.local/lib/python3.8/site-packages && spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 /opt/bitnami/spark/app/src/quality/silver_quality.py"',
        docker_url="unix://var/run/docker.sock",
        network_mode="ecommerce_docker_default",
        mounts=[Mount(source="/d/Projects/data_engineering_projects/ecommerce", target="/opt/bitnami/spark/app", type="bind")],
         environment={
            'SPARK_MODE': 'worker',
            'SPARK_MASTER_URL': 'spark://ecommerce-spark-master:7077'
        }
    )

    # 3. Trigger Gold Batch
    trigger_gold = DockerOperator(
        task_id='trigger_gold_batch',
        image='bitnami/spark:3',
        container_name='airflow_spark_submit_gold',
        api_version='auto',
        auto_remove=True,
        command='bash -c "export HOME=/tmp && pip install --user pandas pyarrow && export PYTHONPATH=$PYTHONPATH:/tmp/.local/lib/python3.8/site-packages && spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 /opt/bitnami/spark/app/src/batch/gold_batch.py"',
        docker_url="unix://var/run/docker.sock",
        network_mode="ecommerce_docker_default",
        mounts=[Mount(source="/d/Projects/data_engineering_projects/ecommerce", target="/opt/bitnami/spark/app", type="bind")],
         environment={
            'SPARK_MODE': 'worker',
            'SPARK_MASTER_URL': 'spark://ecommerce-spark-master:7077'
        }
    )

    trigger_silver >> trigger_quality >> trigger_gold
