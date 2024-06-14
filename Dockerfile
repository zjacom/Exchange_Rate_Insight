FROM apache/airflow:2.7.1

USER root
RUN apt-get update && apt-get install -y tzdata && ln -snf /usr/share/zoneinfo/Asia/Seoul /etc/localtime && echo "Asia/Seoul" > /etc/timezone