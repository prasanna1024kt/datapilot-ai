# DataPilot AI — Deployment and Environment Setup

## 1. Purpose

This document describes the Docker-based local deployment architecture of DataPilot AI, based on the supplied `docker-compose.yml` and Dockerfiles.

It records the services, image build definitions, runtime configuration, volume mounts, ports and deployment considerations visible in those files.

> Scope: This document reflects the supplied configuration. It does not assume that unprovided `.env` values, startup procedures, health checks, or production deployment settings exist.

## 2. Deployment architecture

The supplied Docker Compose configuration defines these service groups:

| Group | Compose services | Purpose |
|---|---|---|
| Database | `postgres` | PostgreSQL 16 database used by Airflow metadata and configured as the MCP audit database endpoint through environment variables |
| Airflow initialization | `airflow-init` | Runs `airflow db migrate` after PostgreSQL becomes healthy |
| Airflow runtime | `airflow`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer` | Airflow API server, scheduler, DAG processor and triggerer |
| Spark | `spark-master`, `spark-worker`, `spark` | Spark standalone master, worker and development/debug container |
| MCP | `mcp-server` | Runs the DataPilot MCP FastAPI application through Uvicorn |

The Compose file defines a named persistent volume, `postgres_data`, for PostgreSQL data.

## 3. Docker image definitions

### 3.1 Spark image

**Dockerfile:** `docker/spark/Dockerfile`

Base image:

```dockerfile
FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu
```

The image installs PyYAML and adds the following JAR dependencies under `/opt/spark/jars`:

| Dependency | Version shown in Dockerfile | Purpose in this deployment |
|---|---:|---|
| Hadoop AWS | 3.4.2 | Hadoop S3A integration |
| AWS SDK bundle | 2.29.52 | AWS SDK classes used by the S3 connector |
| Hadoop MapReduce client core | 3.4.2 | Hadoop MapReduce core classes |
| Iceberg Spark runtime | 1.11.0 | Apache Iceberg runtime for Spark 4.1 and Scala 2.13 |

The Dockerfile checks that the Iceberg, Hadoop AWS and AWS SDK bundle JARs exist, then switches back to the `spark` user.

### 3.2 Airflow image

**Dockerfile:** `docker/airflow/Dockerfile`

The Dockerfile uses a multi-stage build:

1. The first stage uses the Spark 4.1.3 image.
2. The final stage uses `apache/airflow:3.3.1`.
3. It installs CA certificates, curl and OpenJDK 17.
4. It copies `/opt/spark` from the Spark build stage.
5. It sets `JAVA_HOME`, `SPARK_HOME` and `PATH`.
6. It downloads the Hadoop AWS, AWS SDK bundle, Hadoop MapReduce core and Iceberg runtime JARs into the Spark JAR directory.
7. It installs these Python packages:
   - `apache-airflow-providers-apache-spark`
   - `openai`
   - `python-dotenv`
   - `langsmith`
   - `mcp==2.2.0`

The final image runs as the `airflow` user.

**Implementation note:** the supplied Dockerfile sets `JAVA_HOME` to `/usr/lib/jvm/java-17-openjdk-arm64`. This is architecture-specific. Confirm the target container platform before using this image on a different CPU architecture.

### 3.3 MCP server image

**Dockerfile:** `mcp_server/Dockerfile`

The supplied MCP Dockerfile:

1. Uses `datapilotai-spark:latest` as its base image.
2. Switches to `root` to install Python packages.
3. Installs:
   - `mcp==2.2.0`
   - `uvicorn`
   - `psycopg[binary]`
4. Sets `PYTHONPATH=/opt/datapilot`.
5. Sets the working directory to `/opt/datapilot`.
6. Switches back to the `spark` user.

The Docker Compose service builds this Dockerfile and starts the application with Uvicorn using `mcp_server.server:app`.

**Build dependency:** the `datapilotai-spark:latest` image must exist before the MCP image can be built successfully. The supplied Dockerfile does not itself build the Spark base image; the Spark image is defined separately by `docker/spark/Dockerfile`.

## 4. Docker Compose services

### 4.1 PostgreSQL

| Setting | Value |
|---|---|
| Image | `postgres:16` |
| Container name | `datapilot-postgres` |
| Database | `airflow` |
| User | `airflow` |
| Persistent volume | `postgres_data:/var/lib/postgresql/data` |
| Health check | `pg_isready -U airflow` |
| Check interval | 10 seconds |
| Timeout | 5 seconds |
| Retries | 5 |

The supplied Compose file contains a literal development password value for PostgreSQL. Do not reuse this credential pattern for shared or production deployments.

### 4.2 Airflow initialization

| Setting | Value |
|---|---|
| Image | `apache/airflow:3.3.1` |
| Container name | `datapilot-airflow-init` |
| Dependency | PostgreSQL service must be healthy |
| Command | `airflow db migrate` |

The service mounts the DAGs, logs, plugins and local AWS directory. It uses `LocalExecutor`, disables example DAG loading and receives the Airflow Fernet key through a Compose environment variable.

### 4.3 Airflow API server

| Setting | Value |
|---|---|
| Build | `docker/airflow/Dockerfile` |
| Container name | `datapilot-airflow` |
| Dependency | `airflow-init` must complete successfully |
| Command | `api-server` |
| Host port | `8080` |
| Container port | `8080` |

The service mounts DAGs, logs, plugins, AWS configuration, remediation history, Airflow simple-auth password file, agent code, Spark applications and DataPilot configuration.

The supplied environment includes `PYTHONPATH=/opt/datapilot:/opt/spark-apps`, AWS profile/region settings, Airflow database and Fernet settings, and a mounted simple-auth password file.

### 4.4 Airflow scheduler

| Setting | Value |
|---|---|
| Build | `docker/airflow/Dockerfile` |
| Container name | `datapilot-airflow-scheduler` |
| Dependency | `airflow-init` must complete successfully |
| Command | `scheduler` |

The scheduler mounts DAGs, logs, plugins, AWS configuration, Spark applications, DataPilot configuration, results, agents and the Airflow simple-auth password file.

Its environment includes the MCP endpoint `http://mcp-server:8000/mcp`, OpenAI and LangSmith environment-variable references, AWS settings, and Supervisor Airflow connection settings.

### 4.5 Airflow DAG processor

| Setting | Value |
|---|---|
| Build | `docker/airflow/Dockerfile` |
| Container name | `datapilot-airflow-dag-processor` |
| Dependency | `airflow-init` must complete successfully |
| Command | `dag-processor` |

It mounts DAGs, logs, plugins, AWS configuration, agent code, runtime results, Spark applications and DataPilot configuration.

### 4.6 Airflow triggerer

| Setting | Value |
|---|---|
| Image | `apache/airflow:3.3.1` |
| Container name | `datapilot-airflow-triggerer` |
| Dependency | `airflow-init` must complete successfully |
| Command | `triggerer` |

It mounts DAGs, logs, plugins and the local AWS directory.

### 4.7 Spark master

| Setting | Value |
|---|---|
| Build | `docker/spark/Dockerfile` |
| Container name | `datapilot-spark-master` |
| Command | Spark standalone master |
| Spark master port | `7077` |
| Web UI host port | `8081` |
| Web UI container port | `8080` |

It mounts Spark applications, read-only DataPilot configuration and read-only AWS configuration.

### 4.8 Spark worker

| Setting | Value |
|---|---|
| Build | `docker/spark/Dockerfile` |
| Container name | `datapilot-spark-worker` |
| Dependency | `spark-master` |
| Master URL | `spark://spark-master:7077` |
| Command | Spark standalone worker |

It mounts Spark applications, read-only DataPilot configuration and read-only AWS configuration.

### 4.9 Spark development/debug container

| Setting | Value |
|---|---|
| Build | `docker/spark/Dockerfile` |
| Container name | `datapilot-spark` |
| Command | `tail -f /dev/null` |

It remains running as a development/debug container and mounts Spark applications, read-only DataPilot configuration, read-only AWS configuration, read-only results and read-only agent code.

### 4.10 MCP server

| Setting | Value |
|---|---|
| Build | `mcp_server/Dockerfile` |
| Container name | `datapilot-mcp-server` |
| Dependencies | Spark master and worker |
| Host port | `8000` |
| Container port | `8000` |
| Application | `mcp_server.server:app` |
| Server | Uvicorn, host `0.0.0.0` |

The service mounts MCP server code, Spark applications, read-only DataPilot configuration, runtime results, read-only AWS configuration and read-only agent code.

It receives MCP audit database connection settings through environment-variable references. The actual audit database host/name/user/password values are not specified in this document.

## 5. Environment variables and secrets

The Compose file references the following variables for runtime configuration:

| Variable | Use visible in Compose |
|---|---|
| `AIRFLOW_FERNET_KEYRING` | Airflow Fernet key |
| `AWS_PROFILE` | AWS profile for Airflow services |
| `AWS_DEFAULT_REGION` | AWS region for Airflow services |
| `AWS_ACCESS_KEY_ID` | AWS access key environment variable |
| `AWS_SECRET_ACCESS_KEY` | AWS secret access key environment variable |
| `OPENAI_API_KEY` | OpenAI API key passed to the scheduler |
| `LANGSMITH_API_KEY` | LangSmith API key passed to the scheduler |
| `LANGSMITH_TRACING` | LangSmith tracing setting |
| `LANGSMITH_PROJECT` | LangSmith project name |
| `DATAPILOT_ENV` | DataPilot environment setting |
| `MCP_AUDIT_DB_HOST` | MCP audit database host |
| `MCP_AUDIT_DB_PORT` | MCP audit database port |
| `MCP_AUDIT_DB_NAME` | MCP audit database name |
| `MCP_AUDIT_DB_USER` | MCP audit database user |
| `MCP_AUDIT_DB_PASSWORD` | MCP audit database password |

The Compose file also contains fixed values for the Spark profile and region (`datapilot` and `us-east-1`) in Spark and MCP services.

**Secret-handling requirements:**

- Do not commit `.env` files containing real credentials.
- Do not paste API keys, AWS keys, database passwords, Fernet keys, JWT secrets or Airflow password-file contents into documentation or chat.
- Replace hardcoded development credentials and static authentication secrets with managed secret/environment configuration before sharing or deploying outside a local development environment.
- Restrict access to the mounted `secrets/` directory and AWS credential files.

## 6. Volume mounts and data access

The Compose configuration uses bind mounts to share project code and configuration with containers.

| Host path | Container path | Access noted in Compose |
|---|---|---|
| `./dags` | `/opt/airflow/dags` | Read/write mount |
| `./logs` | `/opt/airflow/logs` | Read/write mount |
| `./plugins` | `/opt/airflow/plugins` | Read/write mount |
| `~/.aws` | `/home/airflow/.aws` or `/opt/spark/.aws` | Read-only |
| `./spark` | `/opt/spark-apps` | Read/write mount |
| `./config` | `/opt/datapilot/config` | Read-only |
| `./agents` | `/opt/datapilot/agents` | Read-only in listed mounts |
| `./results` | `/opt/datapilot/results` | Read/write or read-only depending on service |
| `./mcp_server` | `/opt/datapilot/mcp_server` | Read/write mount |
| `./secrets/airflow_simple_auth_passwords.json` | `/opt/airflow/secrets/airflow_simple_auth_passwords.json` | Read-only in scheduler; API server mount shown without explicit read-only flag |

PostgreSQL data is stored in the named Docker volume `postgres_data`.

## 7. Network endpoints

The following host-published ports are defined:

| Component | Host endpoint |
|---|---|
| Airflow API server | `localhost:8080` |
| Spark master | `localhost:7077` |
| Spark master Web UI | `localhost:8081` |
| MCP server | `localhost:8000` |

Within the Compose network, the scheduler is configured to reach MCP at:

```text
http://mcp-server:8000/mcp
```

The Compose file does not publish a PostgreSQL host port.

## 8. Service startup dependencies

The declared dependencies establish this partial startup order:

1. PostgreSQL health check must pass before `airflow-init` runs.
2. `airflow-init` must complete successfully before the Airflow API server, scheduler, DAG processor and triggerer start.
3. Spark worker depends on Spark master.
4. MCP server depends on Spark master and worker.

These are the dependencies explicitly declared in the supplied Compose file. They do not by themselves prove that every downstream application endpoint is ready to accept requests.

## 9. Deployment and operational notes

- The Compose file uses `LocalExecutor` for Airflow.
- Airflow example DAG loading is disabled.
- Airflow metadata database migrations are run by the one-shot `airflow-init` service.
- Spark master and worker use the standalone Spark deployment model.
- The Spark and Airflow images include AWS S3A dependencies; the Spark image also includes the Iceberg runtime JAR.
- AWS configuration directories are mounted read-only into the relevant containers.
- The MCP server is exposed on host port `8000`.
- No Copilot API service appears in the supplied Compose file.
- The supplied Compose configuration does not define a health check for Airflow, Spark or MCP services.

## 10. Security observations

The supplied Compose file includes local-development-style authentication values and a static JWT secret directly in the configuration. It also defines a simple-auth user value in an environment setting. These values must not be treated as production-safe defaults.

Before sharing the repository or deploying beyond a private development environment:

1. Move credentials and signing secrets out of the Compose file.
2. Rotate any real secrets that have been committed or shared.
3. Use strong, unique credentials for PostgreSQL and Airflow authentication.
4. Restrict access to host AWS credentials and mounted secret files.
5. Review which services genuinely require direct AWS access keys versus the mounted AWS profile.
6. Review exposed host ports and limit network access where appropriate.

This document intentionally does not reproduce the literal JWT secret or password values from the supplied Compose file.

## 11. Items requiring confirmation before finalizing the runbook

The following deployment details were not established by the supplied Dockerfiles and Compose file:

- The actual `.env` file contents and which variables are populated (do not share secret values).
- The `mcp_server/Dockerfile` build instructions.
- Whether there is a separate Dockerfile or service definition for the Copilot API.
- The intended first-time build/start command and normal restart procedure.
- The intended Airflow login procedure and how the simple-auth password file is generated.
- Whether PostgreSQL is intended to serve both Airflow metadata and MCP audit storage, and the actual MCP audit database/schema setup.
- Any host-level prerequisites beyond those implied by the Docker images and AWS mounts.
- Verified health-check commands for Airflow, Spark and MCP.

These items should be added only after checking the corresponding repository files or operational procedure.

## 12. Local Docker image inventory (observed)

The user supplied the output of:

```bash
docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.ID}}"
```

The following tagged images were present at the time of inspection:

| Repository | Tag | Image ID |
|---|---|---|
| `datapilotai-mcp-server` | `latest` | `469af34b6471` |
| `datapilotai-airflow-dag-processor` | `latest` | `1282b53af7b1` |
| `datapilotai-airflow` | `latest` | `56a11ae07db2` |
| `datapilotai-airflow-scheduler` | `latest` | `6fc7f7c19d26` |
| `datapilotai-spark-master` | `latest` | `203363eeb3ea` |
| `datapilotai-spark-worker` | `latest` | `203363eeb3ea` |
| `datapilotai-spark` | `latest` | `203363eeb3ea` |
| `postgres` | `16` | `f1c3376c26f2` |
| `apache/airflow` | `3.3.1` | `0c4bcc0370e5` |
| `apache/spark` | `4.1.3-scala2.13-java17-python3-ubuntu` | `9b0a6c2c860f` |
| `hello-world` | `latest` | `5dd0d3e6e255` |

The output also contained multiple `<none>:<none>` images. These are untagged images, commonly called dangling or intermediate images. The listing alone does not establish whether each is safe to remove, so no cleanup action is recommended based only on this output.

### 12.1 Observations

- The Spark master, worker and debug service image names are present and share the same image ID in this snapshot.
- The Airflow API server, scheduler and DAG processor have separate repository tags and image IDs.
- The MCP server image is present.
- The base tag `datapilotai-spark:latest`, referenced in `mcp_server/Dockerfile`, is present in this image listing (image ID `203363eeb3ea`). The Spark master, worker and debug image tags also point to that same image ID in this snapshot.
- This is a point-in-time local image inventory, not a guarantee that the images remain present.

## 13. Running Compose containers (observed)

The user supplied the output of:

```bash
docker-compose ps
```

Nine long-running containers were listed:

| Compose service | Container | Image | Observed status | Published ports |
|---|---|---|---|---|
| `airflow` | `datapilot-airflow` | `datapilotai-airflow` | Up 4 days | `8080:8080` |
| `airflow-dag-processor` | `datapilot-airflow-dag-processor` | `datapilotai-airflow-dag-processor` | Up 4 days | None |
| `airflow-scheduler` | `datapilot-airflow-scheduler` | `datapilotai-airflow-scheduler` | Up 3 days | None |
| `airflow-triggerer` | `datapilot-airflow-triggerer` | `apache/airflow:3.3.1` | Up 7 days | None |
| `mcp-server` | `datapilot-mcp-server` | `datapilotai-mcp-server` | Up 5 days | `8000:8000` |
| `postgres` | `datapilot-postgres` | `postgres:16` | Up 7 days; healthy | Not published to host |
| `spark` | `datapilot-spark` | `datapilotai-spark` | Up 7 days | None |
| `spark-master` | `datapilot-spark-master` | `datapilotai-spark-master` | Up 7 days | `7077:7077`, `8081:8080` |
| `spark-worker` | `datapilot-spark-worker` | `datapilotai-spark-worker` | Up 7 days | None |

### 13.1 Runtime observations and limitations

- All nine listed long-running containers were in `Up` state at the time of the command.
- PostgreSQL is the only container explicitly marked `healthy` in this output. The other `Up` statuses do not by themselves prove application-level readiness.
- Airflow UI/API is published on host port `8080`; Spark Master UI is published on host port `8081`; MCP is published on host port `8000`; Spark standalone master port `7077` is published.
- The Compose file defines an `airflow-init` service, but it is not present in this default `docker-compose ps` output. A one-time initialization container may have exited after completing; confirm with `docker-compose ps -a` if its current state must be recorded.
- This is a point-in-time snapshot. Container uptime does not establish that DAGs, MCP tools, Spark jobs, or external integrations are functioning correctly.
- No service restart, DAG trigger, or application write was performed as part of this inspection.

## 14. Airflow initialization container (observed)

The user supplied the output of:

```bash
docker-compose ps -a airflow-init
```

Observed result:

| Service | Container | Image | Status |
|---|---|---|---|
| `airflow-init` | `datapilot-airflow-init` | `apache/airflow:3.3.1` | Exited (0), 3 days ago |

`Exited (0)` indicates that the initialization container's last process exited successfully. This is consistent with a one-time initialization/migration service that completes and stops; it is not expected to remain running like the Airflow API server or scheduler.

This output does not independently prove the current health of the Airflow metadata database or the other Airflow services. PostgreSQL was separately reported healthy in the `docker-compose ps` snapshot.

No initialization command was rerun.

## 15. Source files reviewed

- `docker-compose.yml`
- `docker/spark/Dockerfile`
- `docker/airflow/Dockerfile`
- `mcp_server/Dockerfile`

The placeholder `readme.md` and `de_readme.md` files were excluded from this deployment reference because they are not project documentation.
