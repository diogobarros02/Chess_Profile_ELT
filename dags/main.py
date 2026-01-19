from airflow import DAG
import pendulum 
from datetime import datetime, timedelta
from api.chess_profile import player_details, save_to_json
from airflow.decorators import dag, task
from airflow.models import Variable
from datawarehouse.dwh.bronze.player_details import bronze_table
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


############## player details DAGs ##############
with DAG(
    dag_id = "profile_produce_json",
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
    dag_id = "profile_update_db_bronze_silver",
    default_args=default_args,
    description="DAG to process JSON file and insert data into bronze layer and silver schema (future do core schema)",
    schedule = "0 15 * * *",
    catchup = False
) as dag:
    bronze = bronze_table()
    silver = silver_table()

    bronze >> silver   # dependency


############## player stats DAGs ##############

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


with DAG(
    dag_id = "profile_stats_db_bronze_silver",
    default_args=default_args,
    description="DAG to process JSON file and insert data into bronze layer and silver schema (future do core schema)",
    schedule = "0 15 * * *",
    catchup = False
) as dag:
    bronze = bronze_table()
    silver = silver_table()

    bronze >> silver   # dependency