from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.pokemon_load import save_to_csv
from plugins.pokemon_transform import get_random_uncaptured_ids, load_captured_ids
from plugins.pokemon_extract import fetch_pokemon_data

default_args = {
    'owner': 'poke-master',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def etl_capture():
    captured_ids = load_captured_ids()
    new_ids = get_random_uncaptured_ids(3, captured_ids)
    
    new_pokemon = []
    for pid in new_ids:
        data = fetch_pokemon_data(pid)
        if data:
            new_pokemon.append(data)
    if new_pokemon:
        save_to_csv(new_pokemon)

with DAG(
    'daily_pokemon_capture',
    default_args=default_args,
    description='Capture 3 Pokémon par jour',
    schedule='@daily',
    start_date=datetime(2025, 6, 6),
    catchup=False,
    tags=['pokemon', 'etl'],
) as dag:
    
    task_capture = PythonOperator(
        task_id='capture_3_pokemon',
        python_callable=etl_capture,
    )

    task_capture
