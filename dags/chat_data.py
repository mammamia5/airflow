from kafka import KafkaConsumer
from json import loads
import pandas as pd

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
    print(f"[{data['sender']}] : {data['message']} (받은 시간 : {data['time']})")
    messages.append(data)  # 데이터를 리스트에 추가

consumer.close()

    # 리스트를 Parquet 파일로 저장
if messages:
    df = pd.DataFrame(messages)
    df.to_parquet(f"chat_data_{pd.Timestamp.now().strftime('%Y%m%d%H%M')}.parquet", engine='pyarrow')
    print("Parquet 파일로 저장 완료!")
