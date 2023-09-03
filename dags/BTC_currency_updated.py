from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.hooks.base import BaseHook

from time import localtime, strftime
from datetime import datetime
import requests
import psycopg2

default_args = {
    "owner": "airflow",
    'start_date': days_ago(1)
}
#Создаем переменную с параметрами
variables = Variable.set(key="currency_load_variables",
                         value={"table_name": "rates",
                                "connection_name": "connection_name",
                                "url":"https://api.exchangerate.host/latest"},
                         serialize_json=True)
dag_variables = Variable.get("currency_load_variables", deserialize_json=True)

"""
#Функция получения Connections из Airflow
"""
def get_conn_credentials(connection_name) -> BaseHook.get_connection:
    conn = BaseHook.get_connection(connection_name)
    return conn

"""
#Функция загрузки данных о курсе по API
"""
def import_rate_connection(**kwargs):
# Параметры запроса
    url = dag_variables.get('url')
    params_dict = {'base':'BTC', 'symbols':'RUB', 'source':'crypto', 'places':'20'}
    try:
        response = requests.get(url, params=params_dict)
    except Exception as err:
        print(f'Error occured: {err}')
        return
    data = response.json()
#Вытаскиваем из ответа нужные данные
    Currency1=data['base']   
    Date = strftime("%Y-%m-%d %H:%M:%S", localtime())
    rates=data['rates']
    for Currency2, Rate in rates.items(): 
        print(Currency2, Rate)
#Подключаемся к контексту таски
    ti = kwargs['task_instance']
    ti.xcom_push(key='results', value={"Date":Date, "Currency1":Currency1, "Currency2":Currency2, "Rate":Rate})

"""
#Функция загрузки полученных данных по курсу в таблицу в Postgres
"""
def insert_data(**kwargs):
    
#Получаем данные из контекста
    task_instance = kwargs['ti']
    results = task_instance.xcom_pull(key='results', task_ids='import')
#Подключаемся к Postgres при помощи Connections в Airflow    
    pg_conn = get_conn_credentials(dag_variables.get('connection_name'))
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = pg_conn.host, pg_conn.port, pg_conn.login, pg_conn.password, pg_conn.schema
    conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
#Загружаем данные и, при необходимости, создаем таблицу    
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS public.rates (Id serial PRIMARY KEY, Date VARCHAR (50), Currency1 VARCHAR (3), Currency2 VARCHAR (3), Rate FLOAT);")
    cursor.execute(f"INSERT INTO {dag_variables.get('table_name')} (Date, Currency1, Currency2, Rate) valueS('{results['Date']}', '{results['Currency1']}', '{results['Currency2']}', '{results['Rate']}');")
    conn.commit()

    cursor.close()
    conn.close()

#определяем даг
with DAG(dag_id = "calc", schedule_interval = "*/10 * * * *",
    default_args = default_args, tags=["1T", "test"], catchup = False) as dag:
        
    dag.doc_md = __doc__

    hello_colleagues = BashOperator(task_id = 'bash',
                    bash_command = "echo 'Hello, World!'")
    
    import_rate = PythonOperator(task_id = "import",
                                                python_callable = import_rate_connection)
    
    insert_rate_postgres= PythonOperator(task_id="insert",
                                                python_callable = insert_data) 
    
#Определяем последовательность тасков    
hello_colleagues>> import_rate >> insert_rate_postgres
      