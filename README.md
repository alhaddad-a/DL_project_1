# DL_project_1 — Medallion Data Lakehouse (Databricks)

End-to-end **Bronze → Silver → Gold** pipeline on Databricks using **Unity Catalog**, **Delta Lake**, and **PySpark**. Raw CRM and ERP CSV files are ingested into a lakehouse, cleaned and conformed in the Silver layer, then modeled into star-schema dimensions and a sales fact table in Gold.

**Repository:** [github.com/alhaddad-a/DL_project_1](https://github.com/alhaddad-a/DL_project_1)

---

## Architecture

```mermaid
flowchart LR
  subgraph sources [Source systems]
    CRM[CRM CSVs]
    ERP[ERP CSVs]
  end

  subgraph bronze [Bronze — raw ingest]
    B1[crm_* tables]
    B2[erp_* tables]
  end

  subgraph silver [Silver — clean & conform]
    S1[crm_customers / products / sales]
    S2[erp_customers / location / category]
  end

  subgraph gold [Gold — analytics]
    D1[dim_customers]
    D2[dim_products]
    F1[fact_sales]
  end

  CRM --> bronze
  ERP --> bronze
  bronze --> silver
  silver --> gold
  D1 --> F1
  D2 --> F1
