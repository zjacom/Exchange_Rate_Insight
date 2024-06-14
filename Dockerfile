FROM apache/airflow:2.7.1

RUN apt-get update \
    && ln -snf /usr/share/zoneinfo/Asia/Seoul /etc/localtime \