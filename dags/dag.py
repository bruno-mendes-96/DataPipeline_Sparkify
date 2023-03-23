from datetime import datetime, timedelta
import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from helpers import DdlQueries


default_args = {
    'owner': 'bruno_mendes',
    'start_date': datetime.datetime.now(),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    'etl_sparkify',
    default_args=default_args,
    description='Load and transform Sparkify Data in Redshift with Airflow'
)

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

create_all_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=DdlQueries.create_all_tables
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    provide_context=False,
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_path="s3://udacity-dend/log_data",
    metadata_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    provide_context=False,
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_path="s3://udacity-dend/song-data/",
    metadata_path="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    insert_statement=SqlQueries.songplay_table_insert,
    truncate=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    insert_statement=SqlQueries.user_table_insert,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    insert_statement=SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    insert_statement=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    insert_statement=SqlQueries.time_table_insert,
    truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    tables_list = ['time', 'artists', 'songs', 'songplays', 'users'],
    dag=dag
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)


start_operator >> create_all_tables
create_all_tables >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table, load_user_dimension_table]
[load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table, load_user_dimension_table]>> run_quality_checks
run_quality_checks >> end_operator

