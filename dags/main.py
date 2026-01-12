from airflow import DAG
import pendulum 
from datetime import datetime, timedelta
from api.chess_profile import player_details, save_to_json

local_time = pendulum.timezone("Europe/Budapest")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2024, 6, 1, tzinfo=local_time),
    "end_date": None,
}

with DAG(
    dag_id = "produce_json",
    default_args=default_args,
    description="DAG to fetch chess.com profile data and save as JSON",
    schedule = "0 2 * * *",
    catchup = False,
) as dag:
    
    player_data = player_details()
    save_json = save_to_json(player_data)

    # Define task dependencies
    player_data >> save_json
    