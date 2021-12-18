from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import json

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

def filter_dataframe(merged):
    print("Filter dataframes!")
    return merged.loc[(merged['sexo'] == 'Mulher') & (merged['idade'] >= 20) & (merged['idade'] <= 40)]

def extract_and_filter_data():
    print("Extract, Prepare and Load data!")
    
    print('Read dataframe dados_cadastrais:')
    data = read_dataframe_from_csv('/usr/local/airflow/data/datalake/bronze/dados_cadastrais.csv')
    print(data.head())
    
    print('Read dataframe ibge:')
    ibge = read_dataframe_from_csv('/usr/local/airflow/data/datalake/bronze/uf_ibge.csv')
    print(ibge.head())

    merged = merge_dataframes(data, ibge)

    filtered = filter_dataframe(merged)

    filtered.to_csv('/usr/local/airflow/data/datalake/silver/dados_cadastrais_ibge.csv', index=False)
    
    return True        


def extract_average_income_all_people():
    print("Extract average income all people!")
    print("Read filtered dataframe ...")
    data = read_dataframe_from_csv('/usr/local/airflow/data/datalake/silver/dados_cadastrais_ibge.csv')

    print("Calculate average income all people:")
    average_income_all_people = data['renda'].mean()
    print(average_income_all_people)

def extract_average_income_by_state(state):
    print(f"Extract average income by state: {state}")
    print("Read filtered dataframe ...")
    data = read_dataframe_from_csv('/usr/local/airflow/data/datalake/silver/dados_cadastrais_ibge.csv')

    print(f"Calculate average income by state: {state}")
    average_income_by_sate = data.loc[data["uf"] == state]['renda'].mean()
    print(average_income_by_sate)

def extract_average_income_by_region(region):
    print(f"Extract average income by region: {region}")
    print("Read filtered dataframe ...")
    data = read_dataframe_from_csv('/usr/local/airflow/data/datalake/silver/dados_cadastrais_ibge.csv')

    print(f"Calculate average income by region: {region}")
    average_income_by_region = data.loc[data["regiao_nome"] == region]['renda'].mean()
    print(average_income_by_region)


def extract_lowest_average_income_by_state():
    print("Extract lowest average income by state!")
    print("Read filtered dataframe ...")
    data = read_dataframe_from_csv('/usr/local/airflow/data/datalake/silver/dados_cadastrais_ibge.csv')

    print("Calculate lowest average income by state:")
    lowest_average_income_by_state = data.groupby('uf')['renda'].mean().sort_values().head(1)
    print(lowest_average_income_by_state.head())

def extract_highest_average_schooling_by_state():
    print("Extract highest average schooling by state!")
    print("Read filtered dataframe ...")
    data = read_dataframe_from_csv('/usr/local/airflow/data/datalake/silver/dados_cadastrais_ibge.csv')

    print("Calculate highest average schooling by state:")
    highest_average_schooling_by_state = data.groupby('uf')['anosesco'].mean().sort_values(ascending=False).head(1)
    print(highest_average_schooling_by_state.head())

def extract_average_schooling_by_state_and_between_ages(state, age_start, age_end):
    print(f"Extract average schooling by state and between ages: {state} - {age_start} - {age_end}")
    print("Read filtered dataframe ...")
    data = read_dataframe_from_csv('/usr/local/airflow/data/datalake/silver/dados_cadastrais_ibge.csv')

    print(f"Calculate average schooling by state and between ages: {state} - {age_start} - {age_end}")
    average_schooling_by_state_and_between_ages = data.loc[(data["uf"] == state) & (data["idade"] >= age_start) & (data["idade"] <= age_end)]['anosesco'].mean()
    print(average_schooling_by_state_and_between_ages)

def extract_average_income_by_region_ages_and_work_force(region, age_start, age_end, work_force):
    print(f"Extract average income by region, ages and work force: {region} - {age_start} - {age_end} - {work_force}")
    print("Read filtered dataframe ...")
    data = read_dataframe_from_csv('/usr/local/airflow/data/datalake/silver/dados_cadastrais_ibge.csv')

    print(f"Calculate average income by region, ages and work force: {region} - {age_start} - {age_end} - {work_force}")
    average_income_by_region_ages_and_work_force = data.loc[(data["regiao_nome"] == region) & (data["idade"] >= age_start) & (data["idade"] <= age_end) & (data["trab"] == work_force)]['renda'].mean()
    print(average_income_by_region_ages_and_work_force)


def extract_average_income_by_mesoregion_in_state(state, number_mesoregion):
    print(f"Extract average income by mesoregion in state: {state}")
    print("Read filtered dataframe ...")
    data = read_dataframe_from_csv('/usr/local/airflow/data/datalake/silver/dados_cadastrais_ibge.csv')

    print(f"Calculate average income by mesoregion in state: {state}")
    
    average_income_by_mesoregion = data.loc[data["sigla"] == state]['renda'].sum() / number_mesoregion
    print(average_income_by_mesoregion)

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
    start_task = PythonOperator(
        task_id="start", 
        python_callable=start, 
        dag=dag
    )

    extract_and_filter_data_task = PythonOperator(
        task_id="extract_and_filter_data", 
        python_callable=extract_and_filter_data, 
        dag=dag,
    )

    extract_average_income_all_people_task = PythonOperator(
        task_id="extract_average_income_all_people",
        python_callable=extract_average_income_all_people,
        dag=dag,
    )

    extract_average_income_by_state_task = PythonOperator(
        task_id="extract_average_income_by_state",
        python_callable=extract_average_income_by_state,
        op_kwargs={'state': 'Distrito Federal'},
        dag=dag,
    )

    extract_average_income_by_region_task = PythonOperator(
        task_id="extract_average_income_by_region",
        python_callable=extract_average_income_by_region,
        op_kwargs={'region': 'Sudeste'},
        dag=dag,
    )

    extract_lowest_average_income_by_state_task = PythonOperator(
        task_id="extract_lowest_average_income_by_state",
        python_callable=extract_lowest_average_income_by_state,
        dag=dag,
    )

    extract_highest_average_schooling_by_state_task = PythonOperator(
        task_id="extract_highest_average_schooling_by_state",
        python_callable=extract_highest_average_schooling_by_state,
        dag=dag,
    )

    extract_average_schooling_by_state_and_between_ages_task = PythonOperator(
        task_id="extract_average_schooling_by_state_and_between_ages",
        python_callable=extract_average_schooling_by_state_and_between_ages,
        op_kwargs={'state': 'Paraná', 'age_start': 25, 'age_end': 30},
        dag=dag,
    )

    extract_average_income_by_region_ages_and_work_force_task = PythonOperator(
        task_id="extract_average_income_by_region_ages_and_work_force",
        python_callable=extract_average_income_by_region_ages_and_work_force,
        op_kwargs={'region': 'Sul', 'age_start': 25, 'age_end': 35, 'work_force': 'Pessoas na força de trabalho'},
        dag=dag,
    )

    extract_average_income_by_mesoregion_in_state_task = PythonOperator(   
        task_id="extract_average_income_by_mesoregion_in_state",
        python_callable=extract_average_income_by_mesoregion_in_state,
        op_kwargs={'state': 'MG', 'number_mesoregion': 12},
        dag=dag,
    )

start_task >> extract_and_filter_data_task >> [
    extract_average_income_all_people_task, 
    extract_average_income_by_state_task, 
    extract_average_income_by_region_task,
    extract_lowest_average_income_by_state_task,
    extract_highest_average_schooling_by_state_task,
    extract_average_schooling_by_state_and_between_ages_task,
    extract_average_income_by_region_ages_and_work_force_task,
    extract_average_income_by_mesoregion_in_state_task
]