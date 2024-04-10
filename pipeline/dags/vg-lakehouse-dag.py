from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.decorators import dag, task_group
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


glue_bucket = "vg-lakehouse-glue"
bronze_glue_job = "bronze-layer-job"
bronze_glue_job_key = "bronze_glue_script.py"
silver_glue_job = "silver-layer-job"
silver_glue_job_key = "silver_glue_script.py"
gold_glue_job = "gold-layer-job"
gold_glue_job_key = "gold_glue_script.py"
glue_iam_role = "vg-glue-role"
delta_path = "s3://vg-lakehouse/delta_jar/delta-core_2.12-2.1.0.jar,s3://vg-lakehouse/delta_jar/delta-storage-2.1.0.jar"
glue_args = {
    "GlueVersion": "4.0",
    "WorkerType": "G.1X",
    "NumberOfWorkers": 2,
    "DefaultArguments": {
        '--extra-jars': delta_path,
        '--extra-py-files': delta_path,
        '--enable-glue-datacatalog': 'true',
    },
}


glue_script_directory = "/opt/airflow/dags/glue-spark"


@dag(
    start_date=datetime(2024, 4, 8),
    catchup=False,
    schedule_interval="0 20 * * *",
    tags=["lakehouse", "glue"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def lakehouse_dag():
    @task_group(group_id='glue_scripts_to_S3', default_args={"aws_conn_id": "aws_conn"})
    def task_group_upload_toS3():
        """
        Upload all the glue scripts to S3
        """
        upload_bronze_job_s3 = LocalFilesystemToS3Operator(
            task_id="upload_bronze_job_to_s3",
            filename=f"{glue_script_directory}/{bronze_glue_job_key}",
            dest_key=bronze_glue_job_key,
            dest_bucket=glue_bucket,
            replace=True,
        )

        upload_silver_job_s3 = LocalFilesystemToS3Operator(
            task_id="upload_silver_job_to_s3",
            filename=f"{glue_script_directory}/{silver_glue_job_key}",
            dest_key=silver_glue_job_key,
            dest_bucket=glue_bucket,
            replace=True,
        )

        upload_gold_job_s3 = LocalFilesystemToS3Operator(
            task_id="upload_gold_job_to_s3",
            filename=f"{glue_script_directory}/{gold_glue_job_key}",
            dest_key=gold_glue_job_key,
            dest_bucket=glue_bucket,
            replace=True,
        )

        upload_bronze_job_s3 >> upload_silver_job_s3 >> upload_gold_job_s3

    @task_group(
        group_id='run_glue_jobs',
        default_args={
            "aws_conn_id": "aws_conn",
            "iam_role_name": glue_iam_role,
            "create_job_kwargs": glue_args,
            "s3_bucket": glue_bucket,
        },
    )
    def task_group_run_job():
        """
        Run the glue scripts
        
        """
        submit_glue_bronze_job = GlueJobOperator(
            task_id="bronze-layer-job",
            job_name=bronze_glue_job,
            script_location=f"s3://{glue_bucket}/{bronze_glue_job_key}",
        )

        submit_glue_silver_job = GlueJobOperator(
            task_id="silver-layer-job",
            job_name=silver_glue_job,
            script_location=f"s3://{glue_bucket}/{silver_glue_job_key}",
        )

        submit_glue_gold_job = GlueJobOperator(
            task_id="gold-layer-job",
            job_name=gold_glue_job,
            script_location=f"s3://{glue_bucket}/{gold_glue_job_key}",
        )

        submit_glue_bronze_job >> submit_glue_silver_job >> submit_glue_gold_job

    task_group_upload_toS3() >> task_group_run_job()


lakehouse_dag()
