from airflow import DAG
import pendulum 
from datetime import datetime, timedelta
from api.chess_profile import player_details, save_to_json
from airflow.decorators import dag, task
from airflow.models import Variable
from datawarehouse.dwh.silver.player_details import silver_table
from api.player_stats import extract_usernames_from_db, fetch_player_stats, save_raw_stats

local_time = pendulum.timezone("Europe/Budapest")

USERNAMES = Variable.get(
    "chess_usernames",
    default_var="barrotelli,sousa16"
).split(",")

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
    catchup = False
) as dag:
    
    player_data = player_details(usernames=USERNAMES)
    save_json = save_to_json(player_data)

    # Define task dependencies
    player_data >> save_json
    
with DAG(
    dag_id = "update_db_chess",
    default_args=default_args,
    description="DAG to process JSON file and insert data into staging schema (future do core schema)",
    schedule = "0 15 * * *",
    catchup = False
) as dag:
    update_staging = silver_table()
    #later add other layers
    # Define task dependencies
    
with DAG(
    dag_id = "players_games_stats",
    default_args=default_args,
    description="DAG to fetch chess.com players games and stats",
    schedule = "0 15 * * *",
    catchup = False
 ) as dag :
    usernames = extract_usernames_from_db()
    stats = fetch_player_stats(usernames)
    saved_stats = save_raw_stats(stats)
    usernames >> stats >> saved_stats    