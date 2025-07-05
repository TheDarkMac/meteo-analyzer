from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Configuration par défaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 30),
}


with DAG(
        'bash_dag',
        default_args=default_args,
        schedule='*/1 * * * *',  # Exécution quotidienne
        catchup=False,  # Ne pas rattraper les exécutions passées
        max_active_runs=1,  # Pour éviter les conflits
) as dag:
    # ========== Tâches d'Extraction ==========
    my_bash = BashOperator(
            task_id='echo',
            bash_command = 'echo "Hello World"'
        )
    
    my_bash

    