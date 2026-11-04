# DataPilot AI – Spark Processing Framework

## 1. Overview

DataPilot AI uses a configuration-driven PySpark framework to process datasets across different data layers.

The framework separates configuration management, storage path resolution, data reading, transformations, and writing into independent components.

The central implementation is `DataPipeline`, located in:

`spark/framework/pipeline.py`

The generic framework is designed to be reusable across datasets and processing layers. Supporting components provide additional business-specific and data-quality functionality, but those components are not automatically executed by the generic `DataPipeline`.

## 2. Framework Components

### 2.1 Core Generic Spark Framework

| Component | Implementation | Responsibility |
|---|---|---|
| Data Pipeline | `pipeline.py` | Coordinates configured source resolution, reading, optional transformations, target resolution, and writing |
| Configuration Loader | `config_loader.py` | Loads centralized project variables and layer-specific configurations |
| Path Builder | `path_builder.py` | Resolves source and target storage paths |
| Data Reader | `reader.py` | Reads source datasets using the configured format and options |
| Data Transformer | `transformer.py` | Applies configured DataFrame transformations |
| Data Writer | `writer.py` | Writes processed DataFrames using the configured format and write mode |

### 2.2 Supporting Processing Components

| Component | Implementation | Responsibility |
|---|---|---|
| Business Transformer | `business_transformer.py` | Provides business-level transformation logic used by business processing jobs |
| Payment Quality | `payment_quality.py` | Contains payment-specific quality functionality |
| DQ Engine | `dq/engine.py` | Executes configured data-quality checks |
| DQ Checks | `dq/checks.py` | Implements individual data-quality checks |

**Component boundary:** the supporting components are separate from the generic `DataPipeline` execution sequence. DQ execution and business-level processing are invoked by their respective jobs or workflows rather than being implicitly run by every `DataPipeline.run()` call.

## 3. DataPipeline Execution

The `DataPipeline` class accepts a SparkSession and a configuration root.

Its `run(layer, table)` method executes the following sequence:

1. Loads centralized project variables.
2. Loads the configuration for the requested processing layer and dataset.
3. Resolves the source path and source format.
4. Resolves the target path and target format.
5. Reads source data using `DataReader`.
6. Applies configured transformations using `DataTransformer` when transformations are defined.
7. Resolves write configuration, including write mode and partition strategy.
8. Writes the resulting DataFrame using `DataWriter`.
9. Logs successful pipeline completion.

The generic `DataPipeline` handles configured data reading, optional transformations, and writing. DQ execution, RCA, Gold processing, Iceberg operations, and remediation are handled by separate components and workflows.

## 4. Configuration Management

The Configuration Loader reads the centralized project variables and the configuration associated with the requested processing layer and dataset.

The project variables provide shared settings such as:
- Project and environment details.
- AWS region.
- Storage type and bucket.
- Configured storage-layer paths.
- Source and target data formats.

Layer-specific configuration files provide dataset processing settings, including source and target definitions, read options, transformations, and write options.

Configuration files are loaded using YAML parsing. The current implementation does not provide comprehensive schema validation or configuration version management.

## 5. Source Resolution

The framework supports raw file sources and sources from other configured processing layers.

- Raw sources use `build_raw_file_path()`.
- Sources from other configured layers use `build_layer_path()`.

Source formats are resolved from the centralized storage format configuration.

This keeps source path and format definitions outside the generic processing code and allows the same framework to be reused across datasets.

## 6. Target Resolution

Target paths and formats are dynamically generated through `PathBuilder` and centralized configuration.

The configured layer determines the target storage location, while the centralized format settings determine the target format.

This approach avoids hard-coding individual dataset paths in the generic pipeline implementation.

## 7. Transformations

Transformations are optional and are driven by the dataset configuration.

The Data Transformer supports configured DataFrame operations such as:
- Standardizing column names.
- Trimming string values.
- Casting columns to configured data types.
- Converting timestamp fields.
- Removing duplicates.
- Applying configured null handling.
- Applying filters.
- Dropping configured columns.

The current transformer skips configured operations when referenced columns are missing in relevant cases. The implementation does not provide comprehensive protection against every possible transformation configuration error or column-name collision.

Transformations are applied only when transformation configuration is present.

## 8. Writing and Partition Strategy

The Data Writer writes the resulting DataFrame using the configured output format and write mode.

The default write mode is `overwrite` when no write mode is explicitly configured.

The writer receives partition strategy and partition settings from the layer configuration.

**Partitioning clarification:** the current partition strategy controls Spark execution partitions through operations such as `coalesce` or `repartition`. It does not implement storage-level partitioning using Spark's `partitionBy()`.

## 9. Supporting Business and Data Quality Components

The framework repository also contains components used by separate processing jobs.

### 9.1 Business Transformer

`business_transformer.py` provides business-level transformation logic. It is used by business processing code and is not automatically invoked by the generic `DataPipeline`.

### 9.2 Payment Quality

`payment_quality.py` contains payment-specific quality logic. It supports payment data processing and validation use cases rather than serving as a generic transformation stage for every dataset.

### 9.3 Data Quality Engine and Checks

The DQ Engine in `dq/engine.py` executes enabled checks from a DQ configuration. The implementations in `dq/checks.py` include checks such as:
- Row count.
- Required columns.
- Null checks.
- Duplicate checks.
- Numeric range checks.

DQ execution is a separate processing component. The generic `DataPipeline.run()` sequence does not automatically execute the DQ Engine.

Detailed DQ behavior and check semantics are documented separately in:

`docs/pipeline/data-quality.md`

## 10. Design Principles

The Spark Processing Framework follows these design principles:

- **Configuration-driven processing:** dataset behavior is controlled through configuration files.
- **Reusable PySpark components:** reading, transformation, path resolution, and writing are separated into independent modules.
- **Separation of responsibilities:** the generic pipeline coordinates common processing steps while specialized jobs handle business, DQ, Gold, and remediation workflows.
- **Centralized storage path management:** source and target paths are resolved through shared configuration and path-building logic.
- **Configurable source and target formats:** formats are resolved from centralized settings.
- **Dataset reuse:** the same generic processing flow can be applied to multiple datasets and configured layers.

## 11. Processing Boundaries

The Spark framework is one part of the wider DataPilot AI data platform.

The following responsibilities are implemented outside the generic `DataPipeline` sequence:

| Capability | Responsibility |
|---|---|
| Data Quality | Executes configured DQ checks and produces DQ results |
| RCA | Investigates DQ failures and produces root-cause findings |
| Gold processing | Builds curated Gold datasets through separate processing jobs |
| Iceberg operations | Manages Iceberg table creation, merge, and related table operations |
| Remediation | Validates and coordinates approved data-remediation operations |
| Supervisor | Assesses pipeline and remediation workflow outcomes |

These components can use Spark and the framework's data assets, but they should not be interpreted as automatic stages of every generic `DataPipeline.run()` execution.

## 12. Source Code Reference

Core generic framework:
- `spark/framework/pipeline.py`
- `spark/framework/config_loader.py`
- `spark/framework/path_builder.py`
- `spark/framework/reader.py`
- `spark/framework/transformer.py`
- `spark/framework/writer.py`

Supporting processing components:
- `spark/framework/business_transformer.py`
- `spark/framework/payment_quality.py`
- `spark/framework/dq/engine.py`
- `spark/framework/dq/checks.py`

Related documentation:
- `docs/pipeline/data-quality.md`
- `docs/architecture/architecture.md`

## 13. Summary

The DataPilot AI Spark Processing Framework provides a reusable, configuration-driven approach to PySpark data processing. Its generic pipeline coordinates configuration loading, source and target path resolution, data reading, optional transformations, and writing.

Supporting business transformation, payment-quality, and DQ modules provide specialized functionality through separate jobs and workflows. Gold processing, Iceberg operations, RCA, remediation, and Supervisor assessment also remain distinct platform capabilities.

This separation keeps the generic processing framework focused while allowing DataPilot AI to support specialized data engineering and DataOps workflows.
