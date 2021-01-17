from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
from pandas.io.json import json_normalize 
from airflow.operators.email_operator import EmailOperator

data_json=[]

default_args = {
    'owner': 'leander',
    'depends_on_past': False,
    'start_date': datetime.today() - timedelta(days=13),
    'email': ['leandersavio@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG('MYDAG', default_args=default_args,schedule_interval='0 11 * * *')

def fetchDataAPI():
    symbols=['NFLX','MSFT','GOOGL','FB','AMZN','BABA','TSLA']
    
    for val in symbols:
        r = requests.get('https://finnhub.io/api/v1/news-sentiment?symbol='+str(val)+'&token=bvv9c7f48v6r5v93tap0')
        data_json.append(r.json())
        df = json_normalize(data_json)
        df.to_csv("/usr/local/airflow/stocks.csv")
        

    
def datatransform():
    df=pd.read_csv("/usr/local/airflow/stocks.csv")
    df_cols=df.iloc[:,[1,3,5,6,2,7]]
    df_cols.columns=['NewsScore','Symbol','Buzz','WeekAvgBuzz','BearSentiment','BullSentiment']
    df_cols=df_cols[['Symbol','NewsScore','Buzz','WeekAvgBuzz','BearSentiment','BullSentiment']]
    df_final=df_cols.sort_values(by=['BullSentiment'],ascending=False).reset_index()



fetchDataAPI = PythonOperator(
        task_id="fetch_data_from_API",
        python_callable=fetchDataAPI,
        dag=dag
    )
datatransform = PythonOperator(
        task_id="datatransform",
        python_callable=datatransform,
        dag=dag
    )
    
send_email = EmailOperator(
        task_id='send_email',
        to='leandersavio@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>HELLO,</h3><br><h3> PIPELINE SUCESSFULL</h3> """,
        dag=dag
)

  
    
fetchDataAPI >> datatransform >> send_email



