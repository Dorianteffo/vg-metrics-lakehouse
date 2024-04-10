from datetime import datetime, timedelta
from airflow.decorators import dag, task_group
from airflow.providers.amazon.aws.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

glue_bucket = "vg-lakehouse-glue"
bronze_glue_job = "bronze-layer-job"
bronze_glue_job_key = "bronze_glue_script.py"
silver_glue_job = "silver-layer-job"
silver_glue_job_key = "silver_glue_script.py"
gold_glue_job = "gold-layer-job"
gold_glue_job_key = "gold_glue_script.py"
glue_iam_role = "vg-glue-role"
delta_core_jar_path = "s3://vg-lakehouse/delta_jar/delta-core_2.12-2.1.0.jar"
delta_storage_jar_path = "s3://vg-lakehouse/delta_jar/delta-storage-2.1.0.jar"
glue_script_directory = "/opt/airflow/dags/glue-spark"

@dag(
    start_date=datetime(2024, 4, 8),  
    catchup=False,
    schedule_interval="0 20 * * *",  
    tags=["lakehouse", "glue"],
    default_args = {
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }
)
def lakehouse_dag():
    @task_group(group_id='glue_scripts_to_S3')
    def task_group_upload_toS3():
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

    @task_group(group_id='run_glue_jobs')
    def task_group_run_job():
        create_bronze_glue_job = BashOperator(
            task_id="create_bronze_glue_job",
            bash_command=f"""aws glue create-job --name {bronze_glue_job} --role {glue_iam_role} \ 
                            --command 'python3 {bronze_glue_job_key}' --default-arguments 'GlueVersion=4.0,NumberOfWorkers=2,WorkerType=G.1X, \
                            --extra-jars={delta_core_jar_path},{delta_storage_jar_path},--extra-py-files={delta_core_jar_path},{delta_storage_jar_path}'""",
            env={'AWS_DEFAULT_REGION': 'eu-west-3'}
        )

        create_silver_glue_job = BashOperator(
            task_id="create_silver_glue_job",
            bash_command=f"""aws glue create-job --name {silver_glue_job} --role {glue_iam_role} \ 
                            --command 'python3 {silver_glue_job_key}' --default-arguments 'GlueVersion=4.0,NumberOfWorkers=2,WorkerType=G.1X,\
                            --extra-jars={delta_core_jar_path},{delta_storage_jar_path},--extra-py-files={delta_core_jar_path},{delta_storage_jar_path}'""",
            env={'AWS_DEFAULT_REGION': 'eu-west-3'}
        )

        create_gold_glue_job = BashOperator(
            task_id="create_gold_glue_job",
            bash_command=f"""aws glue create-job --name {gold_glue_job} --role {glue_iam_role} \ 
                            --command 'python3 {gold_glue_job_key}' --default-arguments 'GlueVersion=4.0,NumberOfWorkers=2,WorkerType=G.1X,\
                            --extra-jars={delta_core_jar_path},{delta_storage_jar_path},--extra-py-files={delta_core_jar_path},{delta_storage_jar_path}'""",
            env={'AWS_DEFAULT_REGION': 'eu-west-3'}
        )

        create_bronze_glue_job >> create_silver_glue_job >> create_gold_glue_job

    task_group_upload_toS3() >> task_group_run_job()

lakehouse_dag()