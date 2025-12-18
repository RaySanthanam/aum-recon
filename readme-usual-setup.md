# APACHE AIRFLOW


## _Steps for airflow setup with usual command_

### 1. Create Environment

```sh
    python3 -m venv py_env
    source py_env/bin/activate
```

### 2. Install Airflow, Make sure to use the AIRFLOW_VERSION and PYTHON_VERSION correctly

```sh
    python3 --version
    pip install "apache-airflow[celery]==3.0.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.10.txt"
```

### 3. Create Environment Variables

```sh
    export AIRFLOW_HOME=/Users/ray/Desktop/airflow
    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////Users/ray/Desktop/airflow/airflow.db
```

### 4. Create Airflow DB:

```sh
    airflow db migrate
```

### 5. Open WebServer:

```sh
    airflow api-server
```

### 6. Create dag folder, Create Tasks, Test your pipeline:

```sh
    python dags/FILE_NAME
    airflow migrate db
    airflow dags list
    airflow dags list DAG_NAME

    airflow tasks test DAG_NAME TASK_NAME VALUE
```

## 7. Use Standalone instead of all above steps

```sh
    airflow standalone
```

## _Steps for airflow setup with docker_
