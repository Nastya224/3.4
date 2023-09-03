"""
### DAG documntation
This is a simple ETL data pipeline example which extract BTC rates data from API
 and load it into postgresql.
"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from time import localtime, strftime
from datetime import datetime
import requests
import psycopg2


default_args = {
    "owner": "airflow",
    'start_date': days_ago(1)
}

def import_codes ():
#Парсим данные
#В переменную кладем необязательные параметры запроса
    params_dict = {'base':'BTC', 'symbols':'RUB', 'source':'crypto', 'places':'20'}
    try:
        response = requests.get('https://api.exchangerate.host/latest', params=params_dict)
    except Exception as err:
        print(f'Error occured: {err}')
        return
#Запрашиваем json из ответа
    data = response.json()
#Выводим результат
    print(data)
#Определяем переменные из json для СУБД
    Currency1=data['base']
    current_datetime=datetime.now()
#Переводим текущие дату и время в строковый формат    
    Date = current_datetime.strftime("%S:%M:%H %d-%m-%Y")
    rates=data['rates']
    res = [ (Date, Currency1, Currency2, Rate) for Currency2, Rate in rates.items()]
  
#Подключаемся к СУБД
#Определяем параметры подключения
    conn=psycopg2.connect(dbname='test',
                      user='postgres',
                      password='password',
                      host='db',
                      port='5432')

    cur = conn.cursor()

#Выполняем несколько запросов в СУБД: 1) по созданию таблицы для данных о курсе биткоина 2) вставке спарсенных данных
    cursor=conn.cursor()
    with conn.cursor() as cur:
      cur.execute("CREATE TABLE IF NOT EXISTS public.rates (Id serial PRIMARY KEY, Date VARCHAR (50), Currency1 VARCHAR (3), Currency2 VARCHAR (3), Rate FLOAT);")
      cur.executemany("INSERT INTO public.rates (Date, Currency1, Currency2, Rate) VALUES(%s, %s, %s, %s);", res)
      conn.commit()
      cur.close()

#Определяем даг
with DAG(dag_id = "calc-rates", schedule_interval = "*/10 * * * *",
    default_args = default_args, tags=["1T", "test"], catchup = False) as dag:
        
    dag.doc_md = __doc__

    hello_bash_task = BashOperator(task_id = 'bash_task',
                    bash_command = "echo 'Hello, my giggers!'")
    import_rates_from_api = PythonOperator(task_id = "import_rates", 
                                           python_callable = import_codes)
#Определяем очередность тасков      
hello_bash_task >> import_rates_from_api