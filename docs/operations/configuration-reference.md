# DataPilot AI — Configuration Reference

**Document path:** `docs/operations/configuration-reference.md`  
**Configuration sources reviewed:** `variables.yaml` and `gold_tables.yaml` supplied for this documentation update.

## 1. Purpose

This document records the configuration values and contracts visible in the supplied DataPilot AI YAML files. It covers project metadata, AWS region, S3 storage layers and formats, and the dataset mappings used by the generic incremental Gold loader.

Only the two configuration files supplied for this update are documented as configured facts. Other configuration areas are listed as pending source review rather than inferred.

## 2. Configuration inventory

| Configuration file | Responsibility | Status |
|---|---|---|
| `variables.yaml` | Project metadata, AWS region, S3 bucket, layer paths and file formats | Reviewed |
| `gold_tables.yaml` | Glue catalog/database, Gold warehouse and Silver-to-Gold dataset mappings | Reviewed |
| `gold/olist_order_summary.yaml` | Olist business-summary source/target mappings and transformations | Reviewed |
| `quarantine/olist_order_payments.yaml` | Payment quarantine table settings and validation controls | Reviewed |
| Pipeline/DQ YAML files | Pipeline source/target and data-quality rules | DQ rules reviewed; pipeline dataset YAMLs pending |
| Remediation YAML files | Remediation workflow settings | Not supplied for this document |
| Copilot settings and environment variables | OpenAI, Pinecone, LangSmith and MCP configuration | Not supplied for this document |

## 3. `variables.yaml`

### 3.1 Project metadata

```yaml
project:
  name: datapilot-ai
  environment: dev
```

| Key | Configured value |
|---|---|
| `project.name` | `datapilot-ai` |
| `project.environment` | `dev` |

### 3.2 AWS settings

```yaml
aws:
  region: us-east-1
```

The configured AWS region is `us-east-1`.

The supplied YAML does not contain AWS access keys or secret credentials. Credential provisioning is therefore outside the scope of this file.

### 3.3 Storage settings

```yaml
storage:
  type: s3
  bucket: datapilot-ai-data-practice
```

| Key | Configured value |
|---|---|
| `storage.type` | `s3` |
| `storage.bucket` | `datapilot-ai-data-practice` |

### 3.4 Storage layers

```yaml
layers:
  raw: raw
  bronze: processed/bronze
  silver: processed/silver
  gold: processed/gold
  quarantine: processed/quarantine
```

The layer values are relative prefixes within the configured S3 bucket.

| Layer | Prefix | Resolved S3 location |
|---|---|---|
| Raw | `raw` | `s3://datapilot-ai-data-practice/raw` |
| Bronze | `processed/bronze` | `s3://datapilot-ai-data-practice/processed/bronze` |
| Silver | `processed/silver` | `s3://datapilot-ai-data-practice/processed/silver` |
| Gold | `processed/gold` | `s3://datapilot-ai-data-practice/processed/gold` |
| Quarantine | `processed/quarantine` | `s3://datapilot-ai-data-practice/processed/quarantine` |

The resolved locations above are formed from the supplied bucket and layer prefixes. They describe the configured path convention, not a verification that each S3 prefix currently exists.

### 3.5 Storage formats

```yaml
formats:
  raw: csv
  bronze: parquet
  silver: parquet
  gold: parquet
```

| Layer | Configured format |
|---|---|
| Raw | CSV |
| Bronze | Parquet |
| Silver | Parquet |
| Gold | Parquet |
| Quarantine | Not specified in `variables.yaml` |

Do not assume a quarantine file format from this file; the quarantine-specific configuration was not included in the supplied material.

## 4. `gold_tables.yaml`

### 4.1 Gold catalog settings

```yaml
gold:
  catalog: glue_catalog
  database: datapilot
  warehouse: s3a://datapilot-ai-data-practice/gold
```

| Key | Configured value |
|---|---|
| `gold.catalog` | `glue_catalog` |
| `gold.database` | `datapilot` |
| `gold.warehouse` | `s3a://datapilot-ai-data-practice/gold` |

The generic incremental Gold loader uses this configuration to resolve its catalog, database, warehouse and dataset mappings.

### 4.2 Dataset configuration contract

Each entry under `gold.tables` contains:

| Key | Meaning |
|---|---|
| `enabled` | Whether the dataset is enabled for the generic Gold loader |
| `source_path` | Silver Parquet source location |
| `target_table` | Gold Iceberg table name |
| `keys` | Business key columns used for matching and uniqueness validation |
| `validate_source_duplicates` | Configured source-duplicate validation flag |

All nine supplied datasets have `enabled: true` and `validate_source_duplicates: true`.

### 4.3 Configured dataset mappings

| Dataset | Silver source path suffix | Gold target table | Business keys |
|---|---|---|---|
| `olist_customers` | `processed/silver/olist_customers` | `olist_customers` | `customer_id` |
| `olist_geolocation` | `processed/silver/olist_geolocation` | `olist_geolocation` | `geolocation_zip_code_prefix`, `geolocation_lat`, `geolocation_lng` |
| `olist_order_items` | `processed/silver/olist_order_items` | `olist_order_items` | `order_id`, `order_item_id` |
| `olist_order_payments` | `processed/silver/olist_order_payments` | `olist_order_payments` | `order_id`, `payment_sequential` |
| `olist_order_reviews` | `processed/silver/olist_order_reviews` | `olist_order_reviews` | `review_id`, `order_id` |
| `olist_orders` | `processed/silver/olist_orders` | `olist_orders` | `order_id` |
| `olist_product_category_translation` | `processed/silver/olist_product_category_translation` | `olist_product_category_translation` | `product_category_name` |
| `olist_products` | `processed/silver/olist_products` | `olist_products` | `product_id` |
| `olist_sellers` | `processed/silver/olist_sellers` | `olist_sellers` | `seller_id` |

All source paths use the `s3a://datapilot-ai-data-practice/` prefix in the supplied configuration.

### 4.4 Business-key behavior

The configured `keys` define the business-key columns for each dataset. In the generic incremental Gold loader, these keys are used to:

- Validate that configured key columns exist in the Silver source.
- Reject source rows with NULL business-key values.
- Detect duplicate source key groups when source-duplicate validation is enabled.
- Build the Iceberg MERGE match condition.
- Validate Gold target uniqueness after the load.

The keys should represent the intended row identity for each target table. Changing a key can change MERGE matching and therefore needs code/configuration review.

## 5. Payment quarantine configuration

**Configuration file:** `config/quarantine/olist_order_payments.yaml`

The supplied configuration is:

```yaml
dataset: olist_order_payments

quarantine:
  enabled: true
  catalog: glue_catalog
  database: datapilot
  table: olist_order_payments_quarantine
  format: iceberg

  source_layer: silver
  target_layer: quarantine

  keys:
    - order_id
    - payment_sequential

  validate_source_duplicates: true
```

### 5.1 Configured values

| Key | Configured value |
|---|---|
| `dataset` | `olist_order_payments` |
| `quarantine.enabled` | `true` |
| `quarantine.catalog` | `glue_catalog` |
| `quarantine.database` | `datapilot` |
| `quarantine.table` | `olist_order_payments_quarantine` |
| `quarantine.format` | `iceberg` |
| `quarantine.source_layer` | `silver` |
| `quarantine.target_layer` | `quarantine` |
| `quarantine.keys` | `order_id`, `payment_sequential` |
| `quarantine.validate_source_duplicates` | `true` |

### 5.2 Relationship to the Gold loader

The previously reviewed `gold_incremental_iceberg.py` payment-specific branch expects this quarantine configuration and validates its expected values. It uses the configured payment business keys to verify that invalid payment records are represented in the quarantine table before the relevant Gold changes proceed.

The Gold loader verifies quarantine evidence; it does not create or populate the quarantine records. Quarantine creation/execution belongs to the separate remediation workflow.

The quarantine target layer `quarantine` resolves through `variables.yaml` to the configured prefix `processed/quarantine`. The quarantine table itself is registered in the configured Glue catalog/database as an Iceberg table.

### 5.3 Quarantine table mapping configuration

A second quarantine-related configuration was supplied. It defines the dataset-to-source/target mapping separately from the dataset-level quarantine policy above.

```yaml
quarantine:
  catalog: glue_catalog
  database: datapilot
  tables:
    olist_order_payments:
      enabled: true
      source_path: s3a://datapilot-ai-data-practice/processed/silver/olist_order_payments
      target_table: olist_order_payments_quarantine
      keys:
        - order_id
        - payment_sequential
      validate_source_duplicates: true
```

| Key | Configured value |
|---|---|
| `quarantine.catalog` | `glue_catalog` |
| `quarantine.database` | `datapilot` |
| `quarantine.tables.olist_order_payments.enabled` | `true` |
| `quarantine.tables.olist_order_payments.source_path` | `s3a://datapilot-ai-data-practice/processed/silver/olist_order_payments` |
| `quarantine.tables.olist_order_payments.target_table` | `olist_order_payments_quarantine` |
| `quarantine.tables.olist_order_payments.keys` | `order_id`, `payment_sequential` |
| `quarantine.tables.olist_order_payments.validate_source_duplicates` | `true` |

**Configuration distinction:** the preceding `olist_order_payments.yaml` policy specifies the dataset, enabled state, Iceberg format, source/target layers and expected keys. This mapping configuration specifies the catalog/database and dataset source path/target table mapping. Both point to the same quarantine table and composite key.

The filename for this second mapping configuration was not included with the supplied YAML, so this document describes its structure without inventing a file path.

## 6. Configuration alignment: warehouse path difference

**A configuration difference exists between the two supplied files.**

`variables.yaml` defines the Gold layer prefix as:

```yaml
storage:
  bucket: datapilot-ai-data-practice
  layers:
    gold: processed/gold
```

This resolves to:

```text
s3://datapilot-ai-data-practice/processed/gold
```

`gold_tables.yaml` defines its warehouse as:

```yaml
gold:
  warehouse: s3a://datapilot-ai-data-practice/gold
```

This is a different prefix:

```text
s3a://datapilot-ai-data-practice/gold
```

The previously reviewed implementation uses these settings in different ways:

- `gold_incremental_iceberg.py` reads the warehouse from `gold_tables.yaml`.
- `run_gold.py` constructs its warehouse from the bucket and Gold layer in `variables.yaml`.

Therefore, the generic incremental loader and the Olist business-summary job are configured with different warehouse locations in the supplied configuration.

This document records the difference; it does not change or reconcile either path. Confirm the intended Iceberg warehouse/catalog layout before changing either configuration.

## 7. Operational guidance

1. Keep bucket and layer prefixes consistent with the intended storage layout.
2. Review source paths, target table names and business keys together when adding or changing a Gold dataset.
3. Keep `enabled` explicit for every dataset.
4. Keep `validate_source_duplicates` aligned with the dataset's uniqueness expectations.
5. Treat the Gold warehouse path as an Iceberg catalog configuration, not automatically as the same thing as a dataset's `processed/gold` output prefix.
6. Do not place AWS credentials or API keys in YAML files committed to source control.
7. Validate configuration changes in a non-production environment before running data-writing jobs.

## 8. Data-quality rule configurations

**Directory:** `config/dq/`  
**Files reviewed:** nine dataset-specific YAML files.

Each supplied DQ configuration defines a dataset and one or more checks under `checks`. The enabled checks and their configured columns/keys are summarized below.

### 8.1 Common check pattern

All nine files enable:

- `row_count` with `min: 1`.
- `required_columns` with a dataset-specific column list.
- `null_checks` with a dataset-specific column list.
- `duplicate_check` with dataset-specific business keys.

Eight datasets also enable `range_checks`. `olist_orders` does not define a `range_checks` section.

### 8.2 Dataset check summary

| Dataset | Required columns | Null-check columns | Duplicate keys | Range checks |
|---|---|---|---|---|
| `olist_customers` | `customer_id`, `customer_unique_id`, `customer_zip_code_prefix`, `customer_city`, `customer_state` | `customer_id`, `customer_unique_id` | `customer_id` | `customer_zip_code_prefix`: 1–99999 |
| `olist_geolocation` | `geolocation_zip_code_prefix`, `geolocation_lat`, `geolocation_lng`, `geolocation_city`, `geolocation_state` | `geolocation_zip_code_prefix`, `geolocation_lat`, `geolocation_lng` | `geolocation_zip_code_prefix`, `geolocation_lat`, `geolocation_lng` | Latitude: -90–90; longitude: -180–180 |
| `olist_order_items` | `order_id`, `order_item_id`, `product_id`, `seller_id`, `price`, `freight_value` | `order_id`, `product_id`, `seller_id` | `order_id`, `order_item_id` | `price`: 0–100000; `freight_value`: 0–100000 |
| `olist_order_payments` | `order_id`, `payment_sequential`, `payment_type`, `payment_installments`, `payment_value` | `order_id`, `payment_type`, `payment_value` | `order_id`, `payment_sequential` | `payment_value`: 0–100000; `payment_installments`: 1–100 |
| `olist_order_reviews` | `review_id`, `order_id`, `review_score`, `review_creation_date`, `review_answer_timestamp` | `review_id`, `order_id`, `review_score` | `review_id`, `order_id` | `review_score`: 1–5 |
| `olist_orders` | `order_id`, `customer_id`, `order_status`, `order_purchase_timestamp`, `order_estimated_delivery_date` | `order_id`, `customer_id`, `order_status` | `order_id` | No range checks configured |
| `olist_product_category_translation` | `product_category_name`, `product_category_name_english` | `product_category_name`, `product_category_name_english` | `product_category_name` | No range checks configured |
| `olist_products` | `product_id`, `product_category_name`, `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm` | `product_id` | `product_id` | `product_weight_g`: 0–100000; `product_photos_qty`: 0–100 |
| `olist_sellers` | `seller_id`, `seller_zip_code_prefix`, `seller_city`, `seller_state` | `seller_id` | `seller_id` | `seller_zip_code_prefix`: 1–99999 |

### 8.3 Dataset-specific observations

- `olist_geolocation` uses a three-column duplicate key: ZIP prefix, latitude and longitude.
- `olist_order_items` uses the composite key `order_id` and `order_item_id`.
- `olist_order_payments` uses the composite key `order_id` and `payment_sequential`; its `payment_installments` range is configured as 1 through 100.
- `olist_order_reviews` uses the composite key `review_id` and `order_id`.
- `olist_products` defines a range check for `product_photos_qty`, but that column is not listed in the same file's `required_columns` list. Confirm that the Silver source contains this column and that the DQ engine handles a missing range-check column as intended.
- The pasted terminal output ends the sellers range line as `max: 99999%`. The trailing `%` may be a shell prompt marker rather than part of the YAML value. Confirm the actual file line before treating the percent sign as configuration.

### 8.4 Relationship to the DQ execution engine

The previously reviewed DQ implementation supports `row_count`, `required_columns`, `null_checks`, `duplicate_check` and `range_checks`. The DQ runner reads the dataset configuration, evaluates checks against the Silver dataset and writes a JSON result.

A DQ failure causes the DQ task to fail. In the pipeline orchestration, the corresponding RCA task is configured to run on the failure path. The Supervisor consumes DQ and RCA evidence according to its separate assessment path.

This section records the YAML rules supplied here. It does not independently verify the live Silver schemas or execute the DQ checks.

## 9. Olist order-summary Gold configuration

**Configuration file:** `config/gold/olist_order_summary.yaml`

The supplied configuration is:

```yaml
source:
  type: gold

  tables:
    orders: olist_orders
    order_items: olist_order_items
    payments: olist_order_payments
    customers: olist_customers
    products: olist_products

target:
  type: gold
  table: olist_order_summary

transformations:
  standardize_column_names: true
  trim_strings: true
```

### 9.1 Source table mapping

| Logical input | Configured Gold table |
|---|---|
| `orders` | `olist_orders` |
| `order_items` | `olist_order_items` |
| `payments` | `olist_order_payments` |
| `customers` | `olist_customers` |
| `products` | `olist_products` |

The source layer is configured as `gold`.

### 9.2 Target

| Key | Configured value |
|---|---|
| `target.type` | `gold` |
| `target.table` | `olist_order_summary` |

### 9.3 Transformations

| Transformation | Configured value |
|---|---|
| `standardize_column_names` | `true` |
| `trim_strings` | `true` |

### 9.4 Relationship to `run_gold.py`

The previously reviewed `spark/jobs/run_gold.py` reads the five configured Gold source tables and invokes `BusinessTransformer.build_order_summary(orders, order_items, payments, customers)`.

**Implementation distinction:** although `products` is configured, the reviewed `run_gold.py` loads and counts the Products table but does not pass its DataFrame to `BusinessTransformer.build_order_summary()`. The configuration mapping therefore includes Products, but the current transformation call does not use it as a transformer input.

The runner writes the resulting order summary to the configured Gold target table using its Iceberg create/MERGE logic. Its warehouse is derived from `variables.yaml` storage settings, which differs from the warehouse configured in `gold_tables.yaml` as described in section 7.

**Terminal-output note:** the pasted YAML ended with `trim_strings: true%`. The trailing `%` appears likely to be a terminal prompt artifact. Confirm the actual file contains `trim_strings: true` without the percent sign.

## 10. Repository configuration inventory

The repository listing supplied for this review confirms the following configuration groups:

| Directory/file | Confirmed purpose |
|---|---|
| `config/bronze/*.yaml` | Nine dataset-specific Bronze ingestion configurations |
| `config/silver/*.yaml` | Nine dataset-specific Silver processing configurations |
| `config/dq/*.yaml` | Nine dataset-specific data-quality configurations |
| `config/gold/olist_order_summary.yaml` | Gold order-summary source/target and transformation configuration |
| `config/quarantine/olist_order_payments.yaml` | Payment quarantine policy/configuration |
| `config/quarantine_tables.yaml` | Quarantine table mapping configuration |
| `config/gold_tables.yaml` | Generic Silver-to-Gold table mappings and Iceberg settings |
| `config/variables.yaml` | Shared project, AWS, storage-layer and format settings |
| `config/global_config.yaml` | Global configuration; contents not yet reviewed |

### 10.1 What “pipeline dataset YAML files” means in this repository

The nine files under `config/bronze/` and nine files under `config/silver/` are the pipeline dataset configurations. They are not a separate `config/pipeline/` directory. Their exact settings must be read from the files before documenting their individual source paths, targets, formats, read/write options or transformations.

### 10.2 What “remediation configuration” means in this repository

There is no separate `config/remediation/` directory in the supplied listing. The remediation-related configuration currently identified is:

- `config/quarantine/olist_order_payments.yaml` — payment quarantine policy/settings.
- `config/quarantine_tables.yaml` — quarantine table mapping.

The existence of these files is confirmed, but `quarantine_tables.yaml` has not yet been reviewed in this document. Do not assume other remediation settings or files exist.

### Quarantine table mapping: `config/quarantine_tables.yaml`

The supplied mapping is:

```yaml
quarantine:
  catalog: glue_catalog
  database: datapilot
  tables:
    olist_order_payments:
      enabled: true
      source_path: s3a://datapilot-ai-data-practice/processed/silver/olist_order_payments
      target_table: olist_order_payments_quarantine
      keys:
        - order_id
        - payment_sequential
      validate_source_duplicates: true
```

| Setting | Configured value |
|---|---|
| Catalog | `glue_catalog` |
| Database | `datapilot` |
| Dataset mapping | `olist_order_payments` |
| Enabled | `true` |
| Source path | `s3a://datapilot-ai-data-practice/processed/silver/olist_order_payments` |
| Target table | `olist_order_payments_quarantine` |
| Business keys | `order_id`, `payment_sequential` |
| Validate source duplicates | `true` |

This file maps the Silver payment dataset to the quarantine target table. It is distinct from `config/quarantine/olist_order_payments.yaml`, which contains the payment quarantine policy/settings previously reviewed.

### Verification result: `global_config.yaml` references

Repository searches were run:

```bash
grep -R "global_config.yaml" --include="*.py" .
grep -R "global_config" --exclude-dir=.git --exclude-dir=.venv --exclude-dir=__pycache__ .
```

Both commands returned no matching lines.

**Interpretation:** no literal reference to `global_config.yaml` or `global_config` was found in the searched repository content, excluding `.git`, `.venv` and `__pycache__`. The reviewed Spark framework explicitly loads `variables.yaml`. Based on the searches performed, `global_config.yaml` appears unreferenced by the current repository code/configuration. This is a search-based finding, not proof against references assembled dynamically or files outside the searched project tree.

### 10.3 Configuration review status

- Bronze and Silver dataset YAMLs: filenames confirmed; contents not yet reviewed.
- DQ YAMLs: contents reviewed in the preceding section.
- Gold order-summary YAML: contents reviewed in the preceding section.
- Gold table mappings: contents reviewed in the preceding section.
- Payment quarantine YAML: contents reviewed in the preceding section.
- `quarantine_tables.yaml`: contents reviewed; maps the payment Silver source to its quarantine table.
- `global_config.yaml`: contents reviewed; no literal reference found in the repository search (excluding `.git`, `.venv` and `__pycache__`).
- `variables.yaml`: contents reviewed in the preceding section.

## 11. Global configuration

**Configuration file:** `config/global_config.yaml`

The supplied configuration is:

```yaml
project:
  name: datapilot-ai
  environment: dev

storage:
  type: s3
  bucket: datapilot-ai-data-practice
  region: us-east-1

layers:
  raw: raw
  bronze: bronze
  silver: silver
  gold: gold

formats:
  raw: csv
  bronze: parquet
  silver: parquet
  gold: parquet
```

### 11.1 Project settings

| Key | Value |
|---|---|
| `project.name` | `datapilot-ai` |
| `project.environment` | `dev` |

### 11.2 Storage settings

| Key | Value |
|---|---|
| `storage.type` | `s3` |
| `storage.bucket` | `datapilot-ai-data-practice` |
| `storage.region` | `us-east-1` |

### 11.3 Layer names

| Layer | Configured directory/key |
|---|---|
| Raw | `raw` |
| Bronze | `bronze` |
| Silver | `silver` |
| Gold | `gold` |

### 11.4 File formats

| Layer | Configured format |
|---|---|
| Raw | CSV |
| Bronze | Parquet |
| Silver | Parquet |
| Gold | Parquet |

### 11.5 Relationship to `variables.yaml`

Both `global_config.yaml` and `variables.yaml` contain project and storage-related settings, but their structures and configured layer paths differ.

For example:
- `global_config.yaml` uses top-level `layers` and maps Bronze, Silver and Gold to `bronze`, `silver` and `gold`.
- `variables.yaml` uses `storage.layers` and maps Bronze, Silver and Gold to `processed/bronze`, `processed/silver` and `processed/gold`.

Do not assume one file overrides the other or that they are interchangeable. The consuming code must be inspected to establish which configuration each component reads.

**Terminal-output note:** the pasted output ended with `parquet%`. The `%` appears likely to be the shell prompt appended after the YAML. Confirm the actual file contains `gold: parquet` without the percent sign.

## 12. Bronze dataset configurations

**Directory:** `config/bronze/`

The repository contains nine Bronze YAML files. All nine use a Raw source, a Bronze target and `overwrite` write mode.

### 12.1 Dataset inventory

| Configuration file | Raw source file | Read settings | Partition strategy | Partitions |
|---|---|---|---|---:|
| `olist_customers.yaml` | `olist_customers_dataset.csv` | `header: true`, `inferSchema: true` | `coalesce` | 1 |
| `olist_geolocation.yaml` | `olist_geolocation_dataset.csv` | `header: true`, `inferSchema: true` | `repartition` | 2 |
| `olist_order_items.yaml` | `olist_order_items_dataset.csv` | `header: true`, `inferSchema: true` | `coalesce` | 1 |
| `olist_order_payments.yaml` | `olist_order_payments_dataset.csv` | `header: true`, `inferSchema: true` | `coalesce` | 1 |
| `olist_order_reviews.yaml` | `olist_order_reviews_dataset.csv` | `header: true`, `inferSchema: true`, `quote: '"'`, `escape: '"'`, `multiLine: true` | `coalesce` | 1 |
| `olist_orders.yaml` | `olist_orders_dataset.csv` | `header: true`, `infer_schema: true` | `coalesce` | 1 |
| `olist_product_category_translation.yaml` | `product_category_name_translation.csv` | `header: true`, `inferSchema: true` | `coalesce` | 1 |
| `olist_products.yaml` | `olist_products_dataset.csv` | `header: true`, `inferSchema: true` | `coalesce` | 1 |
| `olist_sellers.yaml` | `olist_sellers_dataset.csv` | `header: true`, `inferSchema: true` | `coalesce` | 1 |

### 12.2 Common configuration pattern

- `source.type` is `raw`.
- `target.type` is `bronze`.
- `write.mode` is `overwrite`.
- Most datasets use `coalesce` to write one partition.
- Geolocation uses `repartition` with two partitions.
- Most files set `header: true` and `inferSchema: true`.

### 12.3 Dataset-specific details and review observations

- **Order reviews:** explicitly configures quote and escape as a double quote and enables multiline CSV reading.
- **Geolocation:** is the only supplied Bronze configuration using `repartition`, with two partitions.
- **Orders:** includes an additional `table.name: olist_orders` field. Its schema-inference key is spelled `infer_schema`, while the other eight files use `inferSchema`.
- **Product category translation:** reads `product_category_name_translation.csv`, rather than a filename prefixed with `olist_`.

**Compatibility check required:** the `infer_schema` versus `inferSchema` spelling difference is present in the supplied YAMLs. Whether both are accepted depends on the configuration loader and Spark read-option handling. Inspect the code that parses these settings before changing either spelling.

**Scope note:** this section documents the YAML values supplied in the conversation. It does not independently verify how the Bronze job interprets every option or whether the configured partition strategy is applied as intended.

## 13. Silver dataset configurations

**Directory:** `config/silver/`

The repository contains nine dataset-specific Silver configurations. All nine read from a Bronze table and target the Silver layer. Each enables duplicate removal with dataset-specific keys.

### 13.1 Dataset transformation matrix

| Dataset | Duplicate keys | Null handling (`drop_if_null`) | Type casts | Timestamp columns |
|---|---|---|---|---|
| `olist_customers` | `customer_id` | `customer_id`, `customer_unique_id` | `customer_zip_code_prefix: integer` | — |
| `olist_geolocation` | `geolocation_zip_code_prefix`, `geolocation_lat`, `geolocation_lng` | Same three columns | ZIP prefix: integer; latitude/longitude: double | — |
| `olist_order_items` | `order_id`, `order_item_id` | `order_id`, `product_id`, `seller_id` | `order_item_id: integer`; `price`, `freight_value: decimal(12,2)` | `shipping_limit_date` |
| `olist_order_payments` | `order_id`, `payment_sequential` | `order_id`, `payment_type` | `payment_sequential`, `payment_installments: integer`; `payment_value: decimal(12,2)` | — |
| `olist_order_reviews` | `review_id`, `order_id` | `review_id`, `order_id`, `review_score` | `review_score: integer` | `review_creation_date`, `review_answer_timestamp` |
| `olist_orders` | `order_id` | `order_id`, `customer_id` | — | `order_purchase_timestamp`, `order_approved_at`, `order_delivered_carrier_date`, `order_delivered_customer_date`, `order_estimated_delivery_date` |
| `olist_product_category_translation` | `product_category_name` | `product_category_name`, `product_category_name_english` | — | — |
| `olist_products` | `product_id` | `product_id` | `product_name_lenght`, `product_description_lenght`, `product_photos_qty`, `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm`: integer | — |
| `olist_sellers` | `seller_id` | `seller_id` | `seller_zip_code_prefix: integer` | — |

### 13.2 Common transformations

All nine configurations specify:
- `source.type: bronze`
- `target.type: silver`
- `transformations.standardize_column_names: true`
- `transformations.remove_duplicates.enabled: true`

Eight configurations also specify `trim_strings: true`. The supplied `olist_order_reviews.yaml` does not include `trim_strings`.

### 13.3 Dataset-specific observations

- **Geolocation:** duplicate identity is defined by the combination of ZIP prefix, latitude and longitude; those same columns are dropped when null.
- **Order items:** includes a timestamp conversion for `shipping_limit_date` and decimal casts for price and freight value.
- **Payments:** casts installment count and payment sequence to integers, and payment value to `decimal(12,2)`.
- **Reviews:** converts two review date fields to timestamps, but does not configure `trim_strings`.
- **Orders:** lists five timestamp columns, including delivery and approval timestamps.
- **Products:** uses the source column spellings `product_name_lenght` and `product_description_lenght`; these spellings are preserved as supplied.
- **Sellers:** casts ZIP prefix to integer.

**Scope note:** this section records the YAML settings as supplied. It does not establish the execution order, exact behavior for invalid casts, duplicate retention strategy, or whether a configured timestamp/cast is successfully applied. Those details require inspection of the Silver transformation implementation.

## 14. Silver transformation implementation

**Implementation file:** `spark/framework/transformer.py`

The supplied `DataTransformer` source confirms how the Silver transformation options are applied.

### 14.1 Execution order

`DataTransformer.apply(df, transformation_config)` applies configured operations in this order:

1. Standardize column names.
2. Trim string columns.
3. Cast configured columns.
4. Convert configured timestamp columns.
5. Remove duplicates.
6. Handle nulls.
7. Apply row filters.
8. Drop configured columns.

Only enabled/present configuration sections are applied. If the transformation configuration is empty or false, the original DataFrame is returned.

### 14.2 Implementation behavior

| Operation | Actual implementation behavior |
|---|---|
| Column standardization | Strips leading/trailing whitespace from names, lowercases them, replaces spaces and hyphens with underscores. |
| String trimming | Applies Spark `trim()` to columns whose schema data type is `string`. |
| Cast columns | Casts a configured column only if that column exists in the DataFrame. |
| Timestamp conversion | Applies `to_timestamp()` only to configured columns that exist. |
| Duplicate removal | Uses `dropDuplicates()` on the configured keys that exist in the DataFrame. |
| Null handling | Drops rows where any existing configured `drop_if_null` column is null; then applies configured `fill` values with `fillna()`. |
| Row filters | Applies each configured expression sequentially using `filter()`. |
| Drop columns | Drops only configured columns that exist in the DataFrame. |

### 14.3 Important implementation observations

- **Missing duplicate keys are not treated as an error.** The implementation filters the configured keys to those present in the DataFrame. If at least one key exists, deduplication uses only the existing subset; if none exist, the DataFrame is returned unchanged.
- **Missing cast and timestamp columns are silently skipped.** The implementation does not fail simply because a configured column is absent.
- **Missing null-check columns are silently skipped.** `dropna()` receives only the configured columns that exist in the DataFrame.
- **Duplicate retention is unspecified.** `dropDuplicates()` is used without a deterministic ordering rule, so the code does not define which duplicate row is retained.
- **Cast and timestamp parse outcomes are not validated here.** This method does not check whether casts or `to_timestamp()` produce null values for malformed inputs.
- **Transformation order matters.** For example, column standardization occurs before casts and duplicate removal, so later configured column names must match the standardized names.
- **Null dropping happens after casts and timestamp conversion.** Values that become null during those conversions may be removed only if their columns are included in `drop_if_null`.
- **`fillna()` is applied after `dropna()`.** A column configured in both `drop_if_null` and `fill` will have rows with nulls in that column dropped before filling.
- **No explicit error is raised for an empty result** in this class; any such validation would need to exist elsewhere in the pipeline.

### 14.4 Relationship to the supplied Silver YAMLs

The Silver YAMLs configure standardization, trimming, casts, timestamps, duplicate keys and null handling. The implementation confirms the order and mechanics above. It also contains optional `filters`, `drop_columns` and null `fill` support, although these options were not present in the nine Silver YAMLs supplied for this review.

**Scope note:** this document describes the supplied `transformer.py` implementation. It does not establish how the pipeline loads each YAML, invokes `DataTransformer.apply()`, or handles Spark exceptions. Those questions require reviewing `config_loader.py` and `pipeline.py`.

## 15. Configuration loader implementation

**Implementation file:** `spark/framework/config_loader.py`

The supplied `ConfigLoader` class confirms how shared variables and layer-specific YAML files are located and loaded.

### 15.1 Configuration root

- The constructor accepts `config_root` as a string.
- It stores the root as a `pathlib.Path` object in `self.config_root`.
- The caller determines the root directory; this file does not define a default root.

### 15.2 YAML loading

`load_yaml(file_path)` opens the supplied path in read mode and parses it with `yaml.safe_load()`.

### 15.3 Shared variables

`load_variables()` reads:

```text
<config_root>/variables.yaml
```

This confirms that this loader reads `variables.yaml` for shared settings. It does not read `global_config.yaml`.

### 15.4 Layer-specific configuration

`load_layer_config(layer, table)` constructs:

```text
<config_root>/<layer>/<table>.yaml
```

For example, calls using `layer="bronze"` and `table="olist_orders"` resolve to:

```text
<config_root>/bronze/olist_orders.yaml
```

If the file does not exist, the method raises `FileNotFoundError` with the missing path. Otherwise, it loads and returns the parsed YAML.

### 15.5 Confirmed behavior and boundaries

- The loader uses `yaml.safe_load()`, not an unsafe YAML loader.
- It explicitly checks file existence for layer-specific YAML files.
- `load_variables()` does not perform an explicit existence check in this class; a missing file would fail when `open()` is called.
- The class loads configuration data; it does not itself execute Spark reads, transformations or writes.
- The supplied implementation does not show validation of required YAML keys, schema/type validation, environment-variable substitution, fallback between configuration files, or merging of global and variables configuration.
- The loader's `layer` argument is used directly as a directory name. The supplied code does not restrict it to an allowlist of layer names.

**Relationship to the earlier configuration observation:** this implementation explicitly loads `variables.yaml` through `load_variables()`. It does not load `global_config.yaml` through either method shown. Whether another component reads `global_config.yaml` must be established by searching the rest of the repository; do not assume it is active or unused based only on this class.

## 16. Pipeline orchestration implementation

**Implementation file:** `spark/framework/pipeline.py`

The supplied `DataPipeline` class shows how configuration loading, path construction, reading, transformation and writing are connected for a requested layer and dataset.

### 16.1 Initialization

When `DataPipeline` is created, it:

1. Creates a `ConfigLoader` using the supplied `config_root`.
2. Loads shared settings through `load_variables()`; this means it reads `variables.yaml`.
3. Initializes `PathBuilder` with those variables.
4. Initializes `DataReader`, `DataWriter` and `DataTransformer`.

This class does not load `global_config.yaml`.

### 16.2 Run flow

`run(layer, table)` performs these steps:

1. Loads `<config_root>/<layer>/<table>.yaml`.
2. Reads the `source` and `target` sections from that YAML.
3. Resolves the source path and source format.
4. Resolves the target path and target format.
5. Reads the source using the YAML `read` options.
6. Applies the YAML `transformations` section when it is non-empty; otherwise, skips transformations.
7. Reads write settings, defaulting `write.mode` to `overwrite`.
8. Passes the DataFrame, target path, format, mode and complete write configuration to `DataWriter.write()`.
9. Logs successful completion after the writer call returns.

### 16.3 Source path and format resolution

- If `source.type` is `raw`, the source path is built with `PathBuilder.build_raw_file_path(source.file)`, and the source format is read from `variables["storage"]["formats"]["raw"]`.
- For any non-raw source type, the source table is `source.table` when provided, otherwise the `table` argument passed to `run()`.
- The non-raw source path is built with `PathBuilder.build_layer_path(source_type, source_table)`.
- The source format is read from `variables["storage"]["formats"][source_type]`.

### 16.4 Target path and format resolution

- The target path is built with `PathBuilder.build_layer_path(target.type, table)`.
- The target table/dataset name used in the target path comes from the `table` argument passed to `run()`.
- The target format is read from `variables["storage"]["formats"][target.type]`.

### 16.5 Read, transform and write behavior

- `read` options are passed to `DataReader.read()` as supplied by the layer YAML.
- `transformations` are passed to `DataTransformer.apply()` when the section is non-empty.
- `write.mode` defaults to `overwrite` if omitted.
- The full `write` dictionary is passed to `DataWriter.write()`, including options such as `partition_strategy` and `partitions`.
- This class logs the partition strategy and partition count when present; the actual write behavior is implemented in `writer.py`, not in this file.

### 16.6 Important implementation observations

- **The YAML `table.name` field is not read by this pipeline code.** For the supplied `config/bronze/olist_orders.yaml`, the `table.name: olist_orders` property has no visible effect in this class. The `table` argument passed to `run()` controls the layer-config filename and target path. For non-raw sources, `source.table` can override the source table name.
- **Raw source filenames come from `source.file`.** The raw file path is built from that value and the Raw format comes from `variables.yaml`.
- **No explicit validation of configuration keys is shown.** Required keys are accessed directly, so missing keys may raise errors such as `KeyError`.
- **The success message follows the writer call.** This class does not independently read back or validate the written output.
- **Partition behavior is delegated.** `pipeline.py` passes write settings to `DataWriter`; it does not itself call `coalesce()` or `repartition()`.
- **Layer names and formats are looked up dynamically.** The requested source and target types must have corresponding entries in `variables.yaml` for these dictionary lookups to succeed.

**Scope note:** this file confirms the orchestration and hand-off behavior. It does not establish the exact Spark reader options, S3 path construction, or physical write/partition behavior; those are implemented in `reader.py`, `path_builder.py` and `writer.py`.

## 17. Data reader implementation

**Implementation file:** `spark/framework/reader.py`

The supplied `DataReader` class provides a small Spark reader wrapper.

### 17.1 Read behavior

`DataReader.read(path, file_format, options=None)`:

1. Creates a Spark reader using `self.spark.read.format(file_format)`.
2. If an options dictionary is supplied, iterates over its key/value pairs.
3. Passes each option to Spark using `.option(key, str(value))`.
4. Loads the supplied path using `.load(path)`.
5. Returns the resulting Spark DataFrame.

### 17.2 Relationship to Bronze configuration

For the Bronze YAMLs, `pipeline.py` passes the `read` section to this method. For example, options such as `header`, `inferSchema`, `quote`, `escape` and `multiLine` are forwarded to Spark as strings.

The reader itself does not define defaults for CSV, Parquet or other formats. The format and path are supplied by `DataPipeline`.

### 17.3 Implementation boundaries

- The reader does not validate that the path exists before calling Spark.
- It does not validate the file format or option names.
- It does not define an explicit schema; it forwards the configured options.
- It does not catch or translate Spark read exceptions.
- It does not perform transformations or write data.

**Scope note:** this documents the supplied `reader.py`. Actual path construction and write behavior are handled by the other framework components.

## 18. Data writer implementation

**Implementation file:** `spark/framework/writer.py`

The supplied `DataWriter` class applies the configured partition strategy and writes the DataFrame.

### 18.1 Write behavior

`DataWriter.write(df, path, file_format, mode="overwrite", write_config=None)`:

1. Replaces a missing or empty `write_config` with an empty dictionary.
2. Reads `partition_strategy`, defaulting to `none`.
3. Reads the optional `partitions` value.
4. Applies the selected partition operation:
   - `coalesce`: calls `df.coalesce(partitions)`.
   - `repartition`: calls `df.repartition(partitions)`.
   - `none`: leaves the DataFrame unchanged.
5. Writes using `df.write.format(file_format).mode(mode).save(path)`.

### 18.2 Validation and errors

- `coalesce` requires a truthy `partitions` value; otherwise it raises `ValueError`.
- `repartition` also requires a truthy `partitions` value; otherwise it raises `ValueError`.
- Any strategy other than `coalesce`, `repartition` or `none` raises `ValueError`.
- The writer does not validate the output path, file format or write mode before calling Spark.
- Spark write exceptions are not caught or translated by this class.

### 18.3 Relationship to the Bronze YAMLs

The supplied Bronze configurations use:
- `coalesce` with one partition for eight datasets.
- `repartition` with two partitions for geolocation.

`pipeline.py` passes the entire `write` section to this writer, so the partition strategy and count in those YAMLs are consumed here.

### 18.4 Important implementation observations

- `coalesce(n)` and `repartition(n)` are invoked before the write; the writer does not use `partitionBy()`. These settings control the DataFrame's number of Spark partitions, not directory partitioning by a column.
- The `partitions` check uses Python truthiness. Values such as `0` or `None` are rejected; the code does not explicitly check that the value is a positive integer.
- With `partition_strategy: none`, a supplied `partitions` value is ignored.
- The default write mode in this method is `overwrite`, but `pipeline.py` also defaults the mode to `overwrite` before calling it.
- This class does not perform a post-write read-back, row-count comparison or output validation.

**Scope note:** this documents the supplied `writer.py`. The exact physical output and Spark execution behavior depend on Spark and the configured storage connector.

## 19. Path builder implementation

**Implementation file:** `spark/framework/path_builder.py`

The supplied `PathBuilder` class constructs S3A paths from the `storage` section of the variables dictionary passed to its constructor.

### 19.1 Initialization

The constructor reads:
- `storage.bucket` into `self.bucket`.
- `storage.layers` into `self.layers`.

This means the paths are based on `variables.yaml`, which `DataPipeline` loads through `ConfigLoader.load_variables()`.

### 19.2 Raw file path

`build_raw_file_path(file_name)` returns:

```text
s3a://<bucket>/<layers.raw>/<file_name>
```

For the supplied configuration, the pattern resolves to:

```text
s3a://datapilot-ai-data-practice/raw/<file_name>
```

### 19.3 Layer path

`build_layer_path(layer, table)`:

1. Checks whether `layer` exists as a key in `self.layers`.
2. Raises `ValueError` if the layer key is not configured.
3. Returns:

```text
s3a://<bucket>/<layers[layer]>/<table>
```

Using the supplied `variables.yaml`, the configured layer paths are:
- Bronze: `s3a://datapilot-ai-data-practice/processed/bronze/<table>`
- Silver: `s3a://datapilot-ai-data-practice/processed/silver/<table>`
- Gold: `s3a://datapilot-ai-data-practice/processed/gold/<table>`
- Quarantine: `s3a://datapilot-ai-data-practice/processed/quarantine/<table>`

### 19.4 Important implementation observations

- The path builder uses `variables.yaml` values; it does not read `global_config.yaml`.
- It constructs `s3a://` paths for both raw files and layer tables.
- It validates the layer key for `build_layer_path()`, but does not separately validate the bucket or configured layer path values.
- `build_raw_file_path()` directly indexes `self.layers['raw']`; a missing `raw` key would raise a key lookup error.
- The class does not check whether an S3 object or directory exists.
- The table and file name are appended as supplied; this class does not sanitize or normalize them.

**Configuration distinction:** `global_config.yaml` maps Bronze/Silver/Gold to `bronze`, `silver` and `gold`, whereas the `variables.yaml` used by this pipeline maps them to `processed/bronze`, `processed/silver` and `processed/gold`. Since `DataPipeline` passes `variables.yaml` into `PathBuilder`, the latter paths are the ones used by this pipeline implementation.

## 20. Configuration not yet documented

The following files/settings were not included in the supplied configuration material and must be reviewed before this document can describe them accurately:

- Pipeline dataset YAML files and DQ rule YAML files
- Remediation configuration files
- Copilot settings and environment-variable names
- Docker Compose environment wiring and secret injection

Do not infer their values from the two files documented above.

## 21. Related implementation and documentation

- `spark/jobs/gold_incremental_iceberg.py`
- `spark/jobs/run_gold.py`
- `framework/payment_quality.py`
- `docs/pipeline/gold-processing.md`
- `docs/pipeline/spark-framework.md`
- `docs/operations/remediation-workflow.md`

## 22. Maintenance

Update this document when project metadata, AWS region, S3 bucket/layer prefixes, storage formats, Gold catalog settings, dataset mappings, business keys or configuration ownership changes.

When configuration values differ between files or runtime components, document the difference explicitly and verify the consuming code before proposing a change.
