# 1. Create a volume for PostgreSQL persistence and create a PostgreSQL container
docker volume create --name airflow-postgresql-data
docker run -d --name airflow-postgresql \
-e POSTGRESQL_USERNAME=airflow \
-e POSTGRESQL_PASSWORD=airflow \
-e POSTGRESQL_DATABASE=airflow \
-v airflow-postgresql-data:/bitnami/postgresql \
-p 5432:5432 bitnami/postgresql:latest


# 2. Create a volume for Redis persistence and create a Redis container
docker volume create --name redis_data
docker run -d --name airflow-redis-new \
-e ALLOW_EMPTY_PASSWORD=yes \
--volume redis_data:/bitnami \
-p 6379:6379 bitnami/redis:latest


# 3. Create volumes for Airflow persistence and launch the container
docker volume create --name airflow-data-panel
docker run -d --name airflow-data-panel -p 8080:8080 \
-e AIRFLOW_FERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho= \
-e AIRFLOW_EXECUTOR=CeleryExecutor \
-e AIRFLOW_DATABASE_HOST=192.168.0.1 \
-e AIRFLOW_DATABASE_PORT_NUMBER=5432 \
-e AIRFLOW_DATABASE_NAME=airflow \
-e AIRFLOW_DATABASE_USERNAME=airflow \
-e AIRFLOW_DATABASE_PASSWORD=airflow \
-e REDIS_HOST=192.168.0.1 \
-e REDIS_PORT_NUMBER=6379 \
-e AIRFLOW_LOAD_EXAMPLES=no \
-e AIRFLOW_PASSWORD=password \
-e AIRFLOW_USERNAME=user \
-e AIRFLOW_EMAIL=info@example.com \
-v /home/subhayu/airflow/airflow.cfg:/opt/bitnami/airflow/airflow.cfg \
-v /home/subhayu/Downloads/Learning_Apache_ASK/Airflow/dags:/opt/bitnami/airflow/dags \
bitnami/airflow:latest


# 4. Create volumes for Airflow Scheduler persistence and launch the container
docker volume create --name airflow_scheduler_data
docker run -d --name airflow-scheduler-1 \
-e AIRFLOW_FERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho= \
-e AIRFLOW_EXECUTOR=CeleryExecutor \
-e AIRFLOW_DATABASE_NAME=airflow \
-e AIRFLOW_DATABASE_USERNAME=airflow \
-e AIRFLOW_DATABASE_PASSWORD=airflow \
-e AIRFLOW_DATABASE_HOST=192.168.0.1 \
-e AIRFLOW_DATABASE_PORT_NUMBER=5432 \
-e AIRFLOW_WEBSERVER_HOST=192.168.0.1 \
-e AIRFLOW_WEBSERVER_PORT_NUMBER=8080 \
-e REDIS_HOST=192.168.0.1 \
-e REDIS_PORT_NUMBER=6379 \
-e AIRFLOW_LOAD_EXAMPLES=no \
-v /home/subhayu/airflow/airflow.cfg:/opt/bitnami/airflow/airflow.cfg \
-v /home/subhayu/Downloads/Learning_Apache_ASK/Airflow/dags:/opt/bitnami/airflow/dags \
bitnami/airflow-scheduler:latest


# 5. Create volumes for Airflow Worker persistence and launch the container
# In this step we can setup the worker from server 192.168.0.1 or 192.168.0.2 or any other host.
docker volume create --name airflow_worker_data
docker run -d --name airflow_worker_1 \
-e AIRFLOW_FERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho= \
-e AIRFLOW_EXECUTOR=CeleryExecutor \
-e AIRFLOW_DATABASE_NAME=airflow \
-e AIRFLOW_DATABASE_USERNAME=airflow \
-e AIRFLOW_DATABASE_PASSWORD=airflow \
-e AIRFLOW_DATABASE_HOST=192.168.0.1 \
-e AIRFLOW_DATABASE_PORT_NUMBER=5432 \
-e AIRFLOW_WEBSERVER_HOST=192.168.0.1 \
-e AIRFLOW_WEBSERVER_PORT_NUMBER=8080 \
-e REDIS_HOST=192.168.0.1 \
-e REDIS_PORT_NUMBER=6379 \
-v /home/subhayu/airflow/airflow.cfg:/opt/bitnami/airflow/airflow.cfg \
-v /home/subhayu/Downloads/Learning_Apache_ASK/Airflow/dags:/opt/bitnami/airflow/dags \
bitnami/airflow-worker:latest


# 6. Setup flower to monitor worker nodes
docker run -p 8888:8888 --name airflow-flower -e CELERY_BROKER_URL=redis://192.168.0.1:6379/1 -e FLOWER_PORT=8888 -d mher/flower
