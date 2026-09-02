# Databricks_projects

### db_flights - Flight Data Pipeline

Production-style data pipeline processing the US DOT Flight Delays dataset using **Databricks, PySpark and Delta Lake**.  
Implements a **Medallion Architecture (Bronze -> Silver -> Gold)** with SCD2 dimensions and a dimensional fact model.  
Uses **Unity Catalog** for data governance and primary/foreign key metadata.  
Deployment and job orchestration are managed with **Declarative Automation Bundles (DABs)**.  
The project demonstrates data ingestion, transformation, dimensional modeling and production-oriented pipeline design.