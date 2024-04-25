from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.decorators import dag
import pendulum
from vertica_utils import VerticaUtils

HOST = Variable.get("vertica_host")
USER = Variable.get("vertica_user")
PASSWORD = Variable.get("vertica_password")
SCHEMA = f'{USER}__STAGING'

# Initialize VerticaUtils with connection details
vertica_utils = VerticaUtils(HOST, USER, PASSWORD)

@dag(schedule_interval=None, start_date=pendulum.parse('2024-04-01'), catchup=False)
def sprint6_dag_load_data_to_staging():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Execute SQL scripts
    execute_scripts_task = PythonOperator(
        task_id='execute_sql_scripts',
        python_callable=vertica_utils.execute_sql_scripts,
        op_kwargs={'script_directory': '/lessons/dags/sql/ddl'},
    )

    load_users = PythonOperator(
        task_id='load_users',
        python_callable=vertica_utils.load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/users.csv',
            'schema': SCHEMA,
            'table': 'users',
            'columns': ['id', 'chat_name', 'registration_dt', 'country', 'age'],
            'type_override': {'age': 'int'}
        },
    )

    load_groups = PythonOperator(
        task_id='load_groups',
        python_callable=vertica_utils.load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/groups.csv',
            'schema': SCHEMA,
            'table': 'groups',
            'columns': ['id', 'admin_id', 'group_name', 'registration_dt', 'is_private'],
            'type_override': {'is_private': 'int'}
        },
    )

    load_dialogs = PythonOperator(
        task_id='load_dialogs',
        python_callable=vertica_utils.load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/dialogs.csv',
            'schema': SCHEMA,
            'table': 'dialogs',
            'columns': ['message_id', 'message_ts', 'message_from', 'message_to', 'message', 'message_group'],
            'type_override': {'message_group': 'int'}
        },
    )

    load_group_log = PythonOperator(
        task_id='load_group_log',
        python_callable=vertica_utils.load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/group_log.csv',
            'schema': SCHEMA,
            'table': 'group_log',
            'columns': ['group_id', 'user_id', 'user_id_from', 'event', 'event_datetime'],
            'type_override': {'group_id': 'int'}
        },
    )

    # Chain tasks in the DAG
    start >> execute_scripts_task >> [load_users, load_groups, load_dialogs, load_group_log] >> end

# Instantiate the DAG
dag = sprint6_dag_load_data_to_staging()