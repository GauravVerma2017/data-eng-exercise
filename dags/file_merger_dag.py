from airflow import DAG
from datetime import *
from merger import config


from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': config.dag_start_date
}

run_date = """{{ macros.ds_format(ds, "%Y-%m-%d","%Y%m%d") }}"""

daily_file_merger_dag = DAG(dag_id='file_merger_dag',
                            default_args=DEFAULT_ARGS,
                            schedule_interval=config.schedule['file_merger']
                            )
input_source_1_sensor_task = FileSensor( task_id= "source_1_file_sensor_task",
                                         poke_interval= 30,
                                         timeout=5 * 30,
                                         filepath= config.file_path['input_source_1']+"data_"+run_date+".json",
                                         dag=daily_file_merger_dag
                                         )
input_source_2_sensor_task = FileSensor(task_id= "source_2_file_sensor_task",
                                        poke_interval= 30,
                                        timeout=5 * 30,
                                        filepath= config.file_path['input_source_2']+"engagement_"+run_date+".csv",
                                        dag=daily_file_merger_dag
                                        )

data_merger_spark_task = SparkSubmitOperator(task_id='data_merger_spark_task',
                                             conn_id='spark_local',
                                             application=f'merger.py',
                                             application_args=[run_date],
                                             name='data_merger_spark_task',
                                             execution_timeout=timedelta(minutes=10),
                                             dag=daily_file_merger_dag
                                             )

data_merger_spark_task.set_upstream([input_source_1_sensor_task, input_source_2_sensor_task])
