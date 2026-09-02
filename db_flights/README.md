# Databricks Flight Data Pipeline

A production-style data pipeline built with **PySpark, Delta Lake and Databricks** using the **US DOT Flight Delays** dataset. The dataset contains flight, airline, airport, delay, cancellation and diversion information. The pipeline transforms raw flight data into a **Medallion Architecture**: Bronze ->  Silver -> Gold. The Gold layer contains SCD2 dimensions and a business-ready flight fact table.

## Architecture

**Bronze → Silver → Gold**

- **Bronze** – raw data ingestion from Kaggle.
- **Silver** – data cleaning, validation and transformations.
- **Gold** – dimensional model with `dim_airlines`, `dim_airports`, `dim_time` and `fct_flights`.
- **Orchestration** – complete pipeline execution through Databricks Jobs.

## Project Structure

```text
db_flights/
├── bronze/       # Raw data ingestion
├── silver/       # Silver transformations
├── gold/         # Dimensions and fact table
├── init_gold/    # Gold table definitions
├── config/       # Pipeline configuration and schemas
└── shared/       # Shared utilities and metadata

resources/        # Databricks Job definitions

pyproject.toml    # Python package and entry points
databricks.yml    # Declarative Automation Bundle configuration
```

## Deployment

The bundle does not store workspace-specific information such as the Databricks host or user name.

Authenticate with your Databricks workspace:

```bash
databricks auth login
```

Deploy the bundle:

```bash
databricks bundle deploy
```

Run the pipeline:

```bash
databricks bundle run flights_master
```

The authenticated user is automatically used for job permissions:

```yaml
permissions:
  - user_name: ${workspace.current_user.userName}
    level: CAN_MANAGE
```

## Job Naming

| Job | Responsibility |
|---|---|
| `bronze_kaggle` | Bronze ingestion from Kaggle |
| `flights_silver` | Silver transformations |
| `flights_gold_init` | Gold data structures |
| `flights_gold` | Gold data modelling |
| `master` | End-to-end orchestration |

## Primary & Foreign Keys

Primary and foreign key constraints are defined in Unity Catalog to document the data model and support data lineage. These constraints are informational and are not enforced by Databricks; data integrity is maintained through pipeline logic and data quality checks.
