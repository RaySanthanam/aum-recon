# APACHE AIRFLOW

### Create .env file with following

```sh
    python generate_test_data.py
```

### Create Sample Data

```sh
    python generate_test_data.py
```

#### Start the docker

```sh
    colima start
    docker compose up -d
```


#### To Clear Cache and Restart

```sh
    docker compose down --volumes --remove-orphans
    docker compose build --no-cache
    docker compose up -d
```


#### To Restart the Container

```sh
    docker restart brokerage-recon-airflow-dag-processor-1 brokerage-recon-airflow-scheduler-1
```

## Project Structure

```
dags/                       - Airflow DAGs directory
  ├── aum_recon.py          - Main reconciliation DAG
  ├── .airflowignore        - Files for Airflow to ignore
  │
  ├── clients/              - SFTP client connections
  │   └── sftp_client.py
  │
  ├── repositories/mongodb/ - MongoDB repository layer
  │   ├── transaction_repo.py   - Transaction aggregations
  │   └── reconciliation_repo.py - Reconciliation results storage
  │
  ├── services/             - Business logic layer
  │   ├── reconciliation_service.py - Reconciliation logic
  │   ├── dbf_reader.py     - DBF file parsing
  │   └── sftp_service.py   - SFTP operations
  │
  └── tests/                - Unit tests (ignored by Airflow via .airflowignore)
```

### Architecture

The project follows a clean layered architecture within the Airflow `dags/` directory:

- **DAG Layer** ([aum_recon.py](dags/aum_recon.py)) - Workflow orchestration only, no business logic
- **Services Layer** ([services/](dags/services/)) - Business logic and data processing
- **Repository Layer** ([repositories/](dags/repositories/)) - DB access and persistence
- **Clients Layer** ([clients/](dags/clients/)) - External system connections (SFTP)
