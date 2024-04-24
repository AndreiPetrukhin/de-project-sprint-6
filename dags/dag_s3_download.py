from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from s3_loader import create_s3_client, download_s3_file

# Bash command template
bash_command_tmpl = """
{% for file in params.files %}
echo 'Showing first 10 lines of: {{ file }}'
head -n 10 {{ file }}
{% endfor %}
"""

# List of file keys
file_keys = ['users.csv', 'groups.csv', 'dialogs.csv', 'group_log.csv']

def setup_download_tasks(file_keys):
    s3_client = create_s3_client()
    bucket_name = 'sprint6'
    local_paths = [f'/data/{key}' for key in file_keys]
    
    for key, path in zip(file_keys, local_paths):
        download_s3_file(s3_client, bucket_name, key, path)
    
    return local_paths

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    's3_file_download_dag',
    default_args=default_args,
    description='A DAG to download files from S3 and print first 10 lines',
    schedule_interval=None,
)

with dag:
    # Download task
    download_task = PythonOperator(
        task_id='download_files',
        python_callable=setup_download_tasks,
        op_kwargs={'file_keys': file_keys},
    )

    # BashOperator to echo the first 10 lines
    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': [f'/data/{key}' for key in file_keys]},
    )

    download_task >> print_10_lines_of_each
