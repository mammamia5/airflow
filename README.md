## 메시지 log 데이터 저장 및 챗봇

### 주요 기능
1. 데이터 저장 - Kafka로 구현한 채팅 메시지 log를 parquet로 저장
2. 시스템 챗봇 - airflow 성공 시 성공 메시지 알림 기능
3. 일정 챗봇 - 특정 시간(칸반미팅 시간) 알림 기능

### 기술스택
- Apache Kafka
- Apache Spark
- Apache Airflow
- Apache Zeppelin

### 설정 및 실행
**환경설정**
```bash
$ cat ~/.zshrc

export AIRFLOW_HOME=~/pj2/airflow
export AIRFLOW__CORE__DAGS_FOLDER=~/pj2/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```
### dags 구조
![image](https://github.com/user-attachments/assets/1071c560-8b58-4b20-a28c-e140025ae233)
