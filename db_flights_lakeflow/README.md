# Databricks Flight Data Pipeline (Lakeflow)

A production-style data pipeline built with **Lakeflow Declarative Pipelines, PySpark and Databricks Asset Bundles** using the **US DOT Flight Delays** dataset (flights, airlines, airports). Raw files are transformed through a **Medallion Architecture**: Bronze -> Silver -> Gold, with SCD2 history for airlines.

### Lineage of the pipeline (TODO)

## Architecture

```
Volume (CSV) --> bronze (Auto Loader) --> silver (cleaned, SCD2) --> gold (dimensional model)
```

| Layer | Tables | What happens |
|---|---|---|
| Bronze | `flights`, `airports`, `airlines` | Config-driven Auto Loader ingestion with explicit schemas and technical columns (`load_date`, `source_table`, `source_file`) |
| Silver | `airlines`, `airports`, `flights` | Renames, typed dates and timestamps, data quality expectations; `airlines` stored as SCD2 |
| Gold | `dim_airlines` | SCD2 dimension with surrogate key and special members (`-1` UNKNOWN, `-999` QUARANTINED) |
| Gold | `dim_airport`, `fact_flights` | *Coming soon* |

## Highlights

- **SCD2 from snapshots** - `silver.airlines` uses `create_auto_cdc_from_snapshot_flow`; a generator job creates a new airlines snapshot with an update, a delete and an insert
- **Incremental processing** - `silver.flights` is a large table(~5.8M rows), so it reads bronze as a stream and each run processes only new rows
- **Data quality** - pipeline expectations that stop the run or only warn, depending on severity
- **Config-driven ingestion** - tables, paths and schemas are defined in one config module
- **Deployment as code** - one bundle with a serverless pipeline and jobs, packaged as a Python wheel

## Project Structure

```text
src/db_flights_lakeflow/
├── bronze/                 # Raw data ingestion
├── silver/                 # Cleaning, validation, SCD2
├── gold/                   # Dimensional model
├── config/                 # Schemas and ingestion config
├── shared/                 # Shared transformations and metadata
└── scd2_data_generator/    # Test snapshot generator for SCD2

resources/                  # Pipeline and job definitions
tests/
pyproject.toml              # Python package
databricks.yaml             # Asset Bundle configuration
```

## Deployment

The bundle does not store workspace-specific information such as the Databricks host or user name.

Upload `flights.csv`, `airlines.csv` and `airports.csv` to `/Volumes/db_flights/raw_data/kaggle_datasets`, then authenticate:

```bash
databricks auth login
```

Deploy the bundle:

```bash
databricks bundle deploy -t dev
```

Run the pipeline:

```bash
databricks bundle run flights_pipeline
```

Run the SCD2 demo (creates a changed airlines snapshot as new file `airlines_001.csv`, then runs the pipeline):
The demo is meant to run once after the initial load. The generator always writes the same file, so a repeated run does not add a new version.
```bash
databricks bundle run flights_scd2_job
```


The authenticated user is automatically used for permissions:

```yaml
permissions:
  - user_name: ${workspace.current_user.userName}
    level: CAN_MANAGE
```

## Resources

| Resource | Responsibility |
|---|---|
| `flights_pipeline` | Bronze, silver and gold tables |
| `flights_scd2_job` | Generates new airlines data batch and runs the pipeline |
| `flights_daily_job` | Daily load; schedule and file-arrival trigger are prepared and commented out |


## Roadmap

- `dim_airport`, `dim_time` and `fact_flights`
- Primary and foreign keys in Unity Catalog (informational)
- Rescued data column in bronze
- Correct day for overnight flights and delays past midnight
- Quarantine table for invalid flights
- Parameterized catalog and paths, `prod` target
- Unit tests and CI