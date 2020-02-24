from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from helpers import CreateTables
from helpers import CheckQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now() - timedelta(minutes=178),
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=300), 
    'email_on_retry': False,
    'depends_on_past': False
    
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval="42 * * * *"
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_creds="aws_credentials",
    createsql=CreateTables.create_staging_events,
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    s3_region="'us-west-2'",
    s3_jsondetails="'s3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_creds="aws_credentials",
    createsql=CreateTables.create_staging_songs,
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    s3_region="'us-west-2'",
    s3_jsondetails="'auto' truncatecolumns"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_creds="aws_credentials",
    createsql=CreateTables.create_songplays,
    insertsql=SqlQueries.songplay_table_insert,
    table="songplays"
)

load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_creds="aws_credentials",
    createsql=CreateTables.create_users,
    insertsql=SqlQueries.user_table_insert,
    table="users",
    append=False
)

load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_creds="aws_credentials",
    createsql=CreateTables.create_songs,
    insertsql=SqlQueries.songs_table_insert,
    table="songs",
    append=False
)

load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_creds="aws_credentials",
    createsql=CreateTables.create_artists,
    insertsql=SqlQueries.artists_table_insert,
    table="artists",
    append=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_creds="aws_credentials",
    createsql=CreateTables.create_time,
    insertsql=SqlQueries.time_table_insert,
    table='"time"',
    append=False
)

startcheck_operator = DummyOperator(task_id='Begin_checks',  dag=dag)

run_users_checks = DataQualityOperator(
    task_id='Run_data_quality_checks_users',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    fact_query=CheckQueries.useridcheck,
    dim_query=CheckQueries.dimcheck    
)

run_songs_checks = DataQualityOperator(
    task_id='Run_data_quality_checks_songs',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    fact_query=CheckQueries.songidcheck,
    dim_query=CheckQueries.dimcheck    
)

run_artists_checks = DataQualityOperator(
    task_id='Run_data_quality_checks_artists',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    fact_query=CheckQueries.artistidcheck,
    dim_query=CheckQueries.dimcheck    
)

run_time_checks = DataQualityOperator(
    task_id='Run_data_quality_checks_time',
    dag=dag,
    redshift_conn_id="redshift",
    table='"time"',
    fact_query=CheckQueries.starttimecheck,
    dim_query=CheckQueries.dimcheck    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_songs_dimension_table
load_songplays_table >> load_users_dimension_table
load_songplays_table >> load_artists_dimension_table
load_songplays_table >> load_time_dimension_table
load_songs_dimension_table >> startcheck_operator
load_users_dimension_table >> startcheck_operator
load_artists_dimension_table >> startcheck_operator
load_time_dimension_table >> startcheck_operator
startcheck_operator >> run_users_checks
startcheck_operator >> run_songs_checks
startcheck_operator >> run_artists_checks
startcheck_operator >> run_time_checks
run_users_checks >> end_operator
run_songs_checks >> end_operator
run_artists_checks >> end_operator
run_time_checks >> end_operator
