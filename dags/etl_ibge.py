from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

def read_dataframe_from_csv(file_path, sep=','):
    spark = SparkSession.builder.appName('ibge').getOrCreate()
    return spark.read.csv(file_path, sep=sep, header=True, inferSchema=True)

def start():
    print("Start!")
    return True

def join_dataframes(data, ibge):
    print("Join dataframes!")

    data.join(ibge, on='UF', how='inner').to_csv('/usr/local/airflow/data/datalake/silver/dados_cadastrais_ibge.csv', index=False)

    return True

def extract_prepare_data():
    print("Extract, Prepare and Load data!")
    
    data = read_dataframe_from_csv('/usr/local/airflow/data/datalake/bronze/dados_cadastrais.csv')
    print('Show of dataframe dados_cadastrais:')
    print(data.show(5))
    
    ibge = read_dataframe_from_csv('/usr/local/airflow/data/datalake/bronze/uf_ibge.csv')
    print('Show of dataframe ibge:')
    print(ibge.show(5))
    
    return True        

default_args = {
    'owner': "Leandro Mendes Costa",
    'depends_on_past': False,
    'start_date': datetime(2021, 12, 16),
}


with DAG(
    "etl_igbe", 
    description="Extract, Prepare and Load data",
    default_args=default_args, 
    schedule_interval=timedelta(1),
    catchup=False,
    max_active_runs=1,
) as dag:
    t = PythonOperator(
        task_id="start", 
        python_callable=start, 
        dag=dag
    )

    t2 = PythonOperator(
        task_id="extract_prepare_data", 
        python_callable=extract_prepare_data, 
        dag=dag
    )

t >> t2