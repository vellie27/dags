from airflow import DAG
from datetime import datetime ,timedelta
from airflow.operators.bash import BashOperator


default_args={
    'owner': 'velyvine',
    'depends_on_past': False,
    'start_date': datetime(2025,8,12),
    'retries': 3,
    'retry_delay':timedelta(minutes=2)
}

with DAG(
    dag_id = 'ayieta_simple_dag',
    default_args= default_args,
    description = 'a simple dag',
    schedule_interval = '@daily',
    catchup = False
)as dag:
    
    
    hello = BashOperator(
        task_id = 'print_hello',
        bash_command = 'echo "hello world"'
    )
    goodbye = BashOperator(
        task_id = 'print_hello',
        bash_command = 'echo "goodbye world"'
    )
    hello >> goodbye
     
    
    
    
    
    
    