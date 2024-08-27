from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent

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
        enable_auto_commit=True,
        group_id='chat_group1',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    # Kafka 메시지를 받아와서 리스트에 저장
    for m in consumer:
        data = m.value
        messages.append(data)
    # Kafka Consumer 종료
    consumer.close()

        # 리스트를 Parquet 파일로 저장
    if messages:
        df = pd.DataFrame(messages)
        file_name = f"~/data/chat_data/chat_mam.parquet"
        df.to_parquet(file_name, index=False)
        print(f"Parquet 파일로 저장 완료: {file_name}")

# Airflow DAG 정의
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 8, 26),
}

with DAG(
    dag_id='kafka_to_parquet_dag',
    default_args=default_args,
    schedule_interval='@hourly',  # 매 시간 실행
    catchup=False,
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
