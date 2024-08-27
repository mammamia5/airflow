from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent
import time

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator,
        BranchPythonOperator
        )

import pandas as pd

# Kafka 데이터를 처리하고 Parquet 파일로 저장하는 함수
def save_kafka_to_parquet():
    from kafka import KafkaConsumer
    from json import loads

    # 데이터를 쌓아둘 리스트
    messages = []

    # Kafka Consumer 설정
    consumer = KafkaConsumer(
        'mammamia3',
        bootstrap_servers=["ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092"],
        auto_offset_reset="earliest",
#        enable_auto_commit=True,
#        group_id='chat_group1',
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        consumer_timeout_ms=1000,
    )

    # Kafka 메시지를 받아와서 리스트에 저장
    #start_time = time.time()
    #max_duration = 1 * 60
    for m in consumer:
        data = m.value
        messages.append(data)

        #if time.time() - start_time > max_duration:
            #break
    # Kafka Consumer 종료
    #consumer.close()

        # 리스트를 Parquet 파일로 저장
    if messages:
        df = pd.DataFrame(messages)
        file_name = f"~/data/chat_data/chat_mam.parquet"
        df.to_parquet(file_name, index=False)
        print(f"Parquet 파일로 저장 완료: {file_name}")
        return True
    return True
# Airflow DAG 정의
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 8, 26),
}

with DAG(
    'chat_parquet',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    description='hello world DAG',
    schedule="10 2 * * *",
    start_date=datetime(2024, 8, 26),
    #end_date=datetime(2016,1,1),
    catchup=True,
    tags=["kafka", "chat"],
#    max_active_runs=1,  # 동시에 실행될 수 있는 최대 DAG 인스턴스 수
#    max_active_tasks=3,  # 동시에 실행될 수 있는 최대 태스크 수
) as dag:
    # PythonOperator로 Kafka 데이터를 Parquet로 저장하는 작업 실행
    save_parquet_task = PythonOperator(
        task_id='save_parquet',
        python_callable=save_kafka_to_parquet
    )
    
    start = EmptyOperator(
        task_id='start'
    )
    end = EmptyOperator(
        task_id='end'
    )


    # flow
    start >> save_parquet_task >> end
