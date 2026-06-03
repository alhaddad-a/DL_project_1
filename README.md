# DL_project_1 — Medallion Data Lakehouse

End-to-end **Bronze → Silver → Gold** pipeline on **Databricks** using **Unity Catalog**, **Delta Lake**, and **PySpark**. Raw CRM and ERP CSV files are ingested into a data lakehouse, cleaned and conformed in Silver, then modeled into star-schema dimensions and a sales fact table in Gold.

---

## Overview

This project implements a classic **medallion architecture** for integrating two operational source systems:

| Source | Files | Role |
|--------|-------|------|
| **CRM** | `cust_info`, `prd_info`, `sales_details` | Customers, products, and sales transactions |
| **ERP** | `CUST_AZ12`, `LOC_A101`, `PX_CAT_G1V2` | Customer attributes, locations, product categories |

All objects live under the Unity Catalog **`datalakehouse`**.

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐
│   CRM (CSV)     │     │   ERP (CSV)     │
└────────┬────────┘     └────────┬────────┘
         │                       │
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │  BRONZE (raw ingest)  │  datalakehouse.bronze.*
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │ SILVER (clean/conform)│  datalakehouse.silver.*
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │ GOLD (dimensional)    │  datalakehouse.gold.*
         │  dim_customers          │
         │  dim_products           │
         │  fact_sales             │
         └───────────────────────┘
```

| Layer | Schema | Purpose |
|-------|--------|---------|
| **Bronze** | `datalakehouse.bronze` | Raw CSV ingest as managed Delta tables |
| **Silver** | `datalakehouse.silver` | Trimming, typing, normalization, column renaming |
| **Gold** | `datalakehouse.gold` | Business-ready star schema for analytics |

---

## Project structure

```
DL_project_1/
├── datasets/                         # Sample source files (upload to Databricks volume)
│   ├── source_crm/
│   │   ├── cust_info.csv
│   │   ├── prd_info.csv
│   │   └── sales_details.csv
│   └── source_erp/
│       ├── CUST_AZ12.csv
│       ├── LOC_A101.csv
│       └── PX_CAT_G1V2.csv
└── scripts/
    ├── init_lakehouse.ipynb          # Catalog, schemas, volume setup
    ├── bronze_layer/
    │   └── bronze.ipynb              # CSV → Bronze Delta tables
    ├── silver_layer/
    │   ├── silver_orchestration.ipynb
    │   ├── crm/                      # CRM silver transforms
    │   └── erp/                      # ERP silver transforms
    └── gold_layer/
        ├── gold_orchestration.ipynb
        ├── gold_dim_customers.ipynb
        ├── gold_dim_products.ipynb
        └── gold_fact_sales.ipynb
```

---

## Prerequisites

- Databricks workspace with **Unity Catalog** enabled
- Permission to create catalog `datalakehouse`, schemas, and volumes
- A cluster or SQL warehouse for notebook execution
- Source CSV files uploaded to the Bronze volume (see Setup below)

---

## Setup

### 1. Initialize the data lakehouse

Run `scripts/init_lakehouse.ipynb`:

- Selects catalog `datalakehouse`
- Creates schemas: `bronze`, `silver`, `gold`
- Creates volume `datalakehouse.bronze.source_systems` for raw files

### 2. Upload source files

Copy the contents of `datasets/` into the Databricks volume:

```
/Volumes/datalakehouse/bronze/source_systems/source_crm/
/Volumes/datalakehouse/bronze/source_systems/source_erp/
```

| System | File | Bronze table |
|--------|------|--------------|
| CRM | `cust_info.csv` | `datalakehouse.bronze.crm_cust_info` |
| CRM | `prd_info.csv` | `datalakehouse.bronze.crm_prd_info` |
| CRM | `sales_details.csv` | `datalakehouse.bronze.crm_sales_details` |
| ERP | `CUST_AZ12.csv` | `datalakehouse.bronze.erp_cust_az12` |
| ERP | `LOC_A101.csv` | `datalakehouse.bronze.erp_loc_a101` |
| ERP | `PX_CAT_G1V2.csv` | `datalakehouse.bronze.erp_px_cat_g1v2` |

---

## Pipeline run order

Execute notebooks in this sequence (manually or as a Databricks Job):

| Step | Notebook | Description |
|------|----------|-------------|
| 1 | `init_lakehouse.ipynb` | One-time infrastructure setup |
| 2 | `bronze_layer/bronze.ipynb` | Ingest all CSVs into Bronze tables |
| 3 | `silver_layer/silver_orchestration.ipynb` | Run all Silver transforms |
| 4 | `gold_layer/gold_orchestration.ipynb` | Build dimensions and fact table |

### Bronze layer

`bronze.ipynb` reads each CSV from the volume with header and inferred schema, then overwrites the corresponding Bronze table.

### Silver layer

`silver_orchestration.ipynb` runs child notebooks via `dbutils.notebook.run`:

| Notebook | Bronze input | Silver output | Key transformations |
|----------|--------------|---------------|---------------------|
| `crm/silver_crm_cust_info` | `crm_cust_info` | `crm_customers` | Trim strings; normalize marital status & gender; drop null IDs; rename columns |
| `crm/silver_crm_prd_info` | `crm_prd_info` | `crm_products` | Parse product key; default null cost; normalize product line; cast dates |
| `crm/silver_crm_sales_details` | `crm_sales_details` | `crm_sales` | Parse `yyyyMMdd` dates; derive price from sales/qty when invalid |
| `erp/silver_erp_cust_az12` | `erp_cust_az12` | `erp_customers` | Strip `NAS` prefix from IDs; validate birth dates; normalize gender |
| `erp/silver_erp_loc_a101` | `erp_loc_a101` | `erp_customer_location` | Clean customer IDs; map country codes (DE, US, etc.) |
| `erp/silver_erp_px_cat_g1v2` | `erp_px_cat_g1v2` | `erp_product_category` | Convert maintenance flag to boolean |

### Gold layer

`gold_orchestration.ipynb` runs notebooks in dependency order:

1. **`gold_dim_customers`** → `datalakehouse.gold.dim_customers`  
   CRM customers enriched with ERP demographics and location.

2. **`gold_dim_products`** → `datalakehouse.gold.dim_products`  
   CRM products joined to ERP categories; surrogate `product_key`.

3. **`gold_fact_sales`** → `datalakehouse.gold.fact_sales`  
   Sales lines joined to dimensions on `customer_id` and `product_number`.

---

## Gold data model

### `dim_customers`

| Column | Description |
|--------|-------------|
| `customer_key` | Surrogate key (`ROW_NUMBER`) |
| `customer_id` | CRM customer ID |
| `customer_number` | Business key (joins CRM ↔ ERP) |
| `first_name`, `last_name` | From CRM |
| `country` | From ERP location |
| `marital_status`, `gender` | CRM preferred; ERP gender as fallback |
| `birthdate` | From ERP |
| `create_date` | From CRM |

### `dim_products`

| Column | Description |
|--------|-------------|
| `product_key` | Surrogate key |
| `product_id`, `product_number` | Product identifiers |
| `product_name`, `product_line` | From CRM |
| `category_id`, `category`, `subcategory` | From ERP |
| `maintenance_flag` | From ERP |
| `start_date` | Product effective date |

### `fact_sales`

| Column | Description |
|--------|-------------|
| `order_number` | Order identifier |
| `product_key`, `customer_key` | Foreign keys to dimensions |
| `order_date`, `ship_date`, `due_date` | Transaction dates |
| `sales_amount`, `quantity`, `price` | Measures |

### Example query

```sql
SELECT
  c.country,
  p.product_line,
  SUM(f.sales_amount) AS total_sales
FROM datalakehouse.gold.fact_sales f
JOIN datalakehouse.gold.dim_customers c ON f.customer_key = c.customer_key
JOIN datalakehouse.gold.dim_products p ON f.product_key = p.product_key
GROUP BY 1, 2
ORDER BY total_sales DESC;
```

---

## Databricks Jobs (recommended)

| Task | Notebook | Frequency |
|------|----------|-----------|
| Init | `init_lakehouse` | Once (or on infra change) |
| Ingest | `bronze_layer/bronze` | Per load / schedule |
| Transform | `silver_layer/silver_orchestration` | After Bronze |
| Model | `gold_layer/gold_orchestration` | After Silver |

Upload the `scripts/` folder to your workspace and point each job task at the corresponding notebook path.

---

## Tech stack

- **Databricks** — notebooks, `dbutils`, `%sql`
- **Apache Spark / PySpark** — transformations
- **Delta Lake** — Silver and Gold table format
- **Unity Catalog** — governance (`datalakehouse` catalog)

---

## Local development notes

- Notebooks target the **Databricks runtime**; paths use Unity Catalog volumes (`/Volumes/datalakehouse/...`), not local disk.
- The `datasets/` folder holds sample CSVs for reference and upload to the Bronze volume.
- Sync notebooks to your Databricks workspace, configure the `datalakehouse` catalog, then follow the pipeline run order above.

---

## License

This project is licensed under the MIT License. See `LICENSE` for details.
