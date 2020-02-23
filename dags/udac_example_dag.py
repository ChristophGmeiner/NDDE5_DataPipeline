from datetime import datetime, timedelta
import configparser
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from create_tables import create_staging_events, create_staging_songs

config = configparser.ConfigParser()
config.read("dwh.cfg")

os.environ['AWS_KEY'] = config["AWS"]["KEY"] 
os.environ['AWS_SECRET'] = config["AWS"]["SECRET"]

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=300),
    'email_on_retry': False,
    'depends_on_past': False
    
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None #'0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_creds="aws_credentials",
    createsql=create_staging_events,
    table="staging_events",
    s3_buchet="udacity-dend",
    s3_key="log_data",
    s3_region="us-west-2",
    s3_jsondetails="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_creds="aws_credentials",
    createsql=create_staging_songs,
    table="staging_songs",
    s3_buchet="udacity-dend",
    s3_key="song_data",
    s3_region="us-west-2",
    s3_jsondetails="'auto' truncatecolumns"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator