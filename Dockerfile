FROM puckel/docker-airflow

# WORKDIR /home/workspace
USER root
RUN apt-get update
RUN apt-get install zip -y
USER airflow
COPY requirements.txt /home/workspace/requirements.txt
RUN pip install -r /home/workspace/requirements.txt
COPY . /home/workspace/
COPY /airflow/ /usr/local/airflow/
EXPOSE 8080