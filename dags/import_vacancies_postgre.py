#Загружаем библиотеки для дага
from airflow import DAG
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "airflow",
    'start_date': days_ago(1)
}

def get_vacancies_from_clickhouse(limit=15000):

    import clickhouse_driver
    clickhouse_params = {
        "host": "host.docker.internal",
        "port": 9000
    }

    try:
        # Устанавливаем соединение с ClickHouse
        conn = clickhouse_driver.connect(**clickhouse_params)
        cursor = conn.cursor()

        query = "SELECT * FROM vacancies2"
        if limit:
            query += f" LIMIT {limit}"

        # Выполняем SQL-запрос
        cursor.execute(query)

        # Получаем все строки результата
        result = cursor.fetchall()

        # Выводим данные
        print("Все данные:")
        for row in result:
            print(row)

    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
    finally:
        # Закрываем соединение с ClickHouse
        conn.close()

if __name__ == "__main__":
    limit=15000
    get_vacancies_from_clickhouse(limit)
 
 
with DAG(dag_id = "import_vacancies_SQLpostgre_engine", schedule_interval = None,
   default_args = default_args, catchup = False) as dag:
        
   import_all = PythonVirtualenvOperator(task_id = "import_vacancies_SQLpostgre_engine", requirements=["clickhouse-driver"],
        system_site_packages=True, python_callable = get_vacancies_from_clickhouse)

import_all