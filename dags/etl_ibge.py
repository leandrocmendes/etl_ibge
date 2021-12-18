from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

def read_dataframe_from_csv(file_path, sep=','):
    """
    Read a csv file and return a pandas dataframe.
    """
    return pd.read_csv(file_path, sep=sep)

def start():
    print("Start!")
    return True

def merge_dataframes(data, ibge):
    print("Merge dataframes!")

    return pd.merge(data, ibge, left_on='uf', right_on='nome', how='inner')

def extract_prepare_data():
    print("Extract, Prepare and Load data!")
    
    data = read_dataframe_from_csv('/usr/local/airflow/data/datalake/bronze/dados_cadastrais.csv')
    print('Head of dataframe dados_cadastrais:')
    print(data.head())
    
    ibge = read_dataframe_from_csv('/usr/local/airflow/data/datalake/bronze/uf_ibge.csv')
    print('Head of dataframe ibge:')
    print(ibge.head())

    merged = merge_dataframes(data, ibge)

    print("Filter dataframes!")

    filtered = merged.loc[(merged['sexo'] == 'Mulher') & (merged['idade'] >= 20) & (merged['idade'] <= 40)]
    print(filtered.head())

    filtered.to_csv('/usr/local/airflow/data/datalake/silver/dados_cadastrais_ibge.csv', index=False)
    
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