from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

@dag(
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["trigger"],
    default_args={
        "owner": "kain",
    }
)
def infra_trigger():
    sensor_culture = ExternalTaskSensor(
        task_id='wait_for_dag_culture',
        external_dag_id='analytics_seoul_culture',  
        mode='reschedule',
        timeout=7200,  
        poke_interval=600   
    )

    sensor_academy = ExternalTaskSensor(
        task_id='wait_for_dag_academy',
        external_dag_id='seoul_academy',
        mode='reschedule',
        timeout=7200,  
        poke_interval=600   
    )

    sensor_cinema = ExternalTaskSensor(
        task_id='wait_for_dag_cinema',
        external_dag_id='seoul_cinema',
        mode='reschedule',
        timeout=7200,  
        poke_interval=600   
    )

    sensor_kindergarden = ExternalTaskSensor(
        task_id='wait_for_dag_kindergarden',
        external_dag_id='seoul_kindergarden',
        mode='reschedule',
        timeout=7200,  
        poke_interval=600   
    )

    sensor_park = ExternalTaskSensor(
        task_id='wait_for_dag_park',
        external_dag_id='seoul_park',
        mode='reschedule',
        timeout=7200,  
        poke_interval=600   
    )

    sensor_school = ExternalTaskSensor(
        task_id='wait_for_dag_school',
        external_dag_id='seoul_school',
        mode='reschedule',
        timeout=7200,  
        poke_interval=600   
    )

    # 서울 인프라 통합 dag 트리거
    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_infra_dag',
        trigger_dag_id='analytics_seoul_infra'  
    )

    # 서울 인프라 dag가 모두 끝나면 서울 인프라 통합 dag 실행
    [sensor_culture, sensor_academy, sensor_cinema, sensor_kindergarden, sensor_park, sensor_school] >> trigger_next_dag

infra_trigger()
