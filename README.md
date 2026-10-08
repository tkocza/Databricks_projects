# Databricks Data Engineering Portfolio

Implementations of the same flight data platform (US DOT Flight Delays) built on Databricks, showing an imperative job-based approach and a declarative Lakeflow approach.

## Projects

| Project | Approach | Highlights |
|---|---|---|
| [db_flights](db_flights) | Imperative, orchestrated with Databricks Jobs | Medallion architecture, SCD2 dimensions with Delta `MERGE`, star schema, Z-ORDER optimization |
| [db_flights_lakeflow](db_flights_lakeflow) | Declarative, Lakeflow Declarative Pipelines | Auto Loader, streaming tables, expectations, AUTO CDC SCD2 from snapshots |

## Tech & Concepts

**Platform**
- Databricks, Unity Catalog, Volumes, serverless compute
- Databricks Asset Bundles (infrastructure as code), Python wheel packaging
- Delta Lake, PySpark

**Data modelling**
- Medallion architecture (Bronze, Silver, Gold)
- Star schema with surrogate keys and special members (`-1` UNKNOWN, `-999` QUARANTINED)
- Slowly Changing Dimensions Type 2 with hash-based change detection
- Informational primary and foreign keys in Unity Catalog

**Orchestration and processing**
- Databricks Jobs, multi-job master workflow, task dependencies, python wheel tasks
- Lakeflow Declarative Pipelines, Auto Loader, streaming tables, materialized views
- Incremental processing, scheduled and file-arrival triggers

**Quality and performance**
- Data quality expectations, missing keys mapped to special members
- OPTIMIZE with Z-ORDER, partitioning, file compaction
- Config-driven ingestion, reusable transformations, technical audit columns

## Approach Comparison

| | db_flights | db_flights_lakeflow |
|---|---|---|
| Ingestion | Batch read, overwrite | Auto Loader, incremental |
| SCD2 | Custom Delta logic | `create_auto_cdc_from_snapshot_flow` |
| Orchestration | Master job of jobs | Pipeline plus jobs |
| Data quality | Business rules in code | Declarative expectations |