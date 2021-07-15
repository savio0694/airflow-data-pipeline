from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from airflow.operators.email import EmailOperator
import papermill as pm


default_args = {
    'owner': 'leander',
    'depends_on_past': False,
    'start_date': datetime.today() - timedelta(days=5),
    'email': ['leandersavio@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG('MYDAG', default_args=default_args,schedule_interval='0 11 * * *')


 

def DataFetch_Transform_Clean():
    pm.execute_notebook(
    '/mnt/c/users/leand/AirflowHome/dags/stock.ipynb',
    '/mnt/c/users/leand/AirflowHome/dags/stock_out.ipynb',
    parameters=dict(alpha=0.6, ratio=0.1)
     
)

DataFetch_Transform_Clean = PythonOperator(
        task_id="run_notebook",
        python_callable=DataFetch_Transform_Clean,
        dag=dag
    )


    

send_email = EmailOperator(
        task_id='send_email',
        to='leandersavio@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>HELLO,</h3><br><h3> PIPELINE SUCESSFULL</h3> """,
        dag=dag
)

  
    
DataFetch_Transform_Clean >> send_email



