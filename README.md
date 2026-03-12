# Claims Processing Pipeline

## Overview
PySpark pipeline that processes insurance claims data — joins claims with policyholder information, derives claim types, priorities, periods, and source system IDs, and enriches records with MD4 hashes via an external API. Built with Python 3.10, PySpark 3.5.8, and Docker.

## Prerequisites
- Docker and Docker Compose
- (Optional) Python 3.10 and Java 21 for local development

## How to Run

### With Docker
```bash
# Build the container
docker-compose build

# Run the pipeline (outputs to data/output/processed_claims.csv)
docker-compose run app

# Run tests
docker-compose run test
```

### Without Docker
```bash
# Requires Python 3.10 and Java 21 installed
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run the pipeline
python -m claims_processor.main

# Run tests
pytest tests/ -v --tb=short

# Run integration tests (requires network access)
pytest -m integration
```

> **Note on MD4:** The local MD4 fallback requires OpenSSL legacy providers to be enabled. The Docker image handles this automatically via `openssl_legacy.cnf`. For local development, your system's OpenSSL may need configuration — see `openssl_legacy.cnf` for the required provider settings.

## Project Structure
```
claims-project/
├── .github/
│   └── workflows/
│       └── tests.yml               # CI pipeline (GitHub Actions)
├── data/
│   ├── input/
│   │   ├── claims_data.csv          # Source claims data
│   │   └── policyholder_data.csv    # Source policyholder data
│   └── output/
│       └── processed_claims.csv     # Pipeline output
├── src/
│   └── claims_processor/
│       ├── __init__.py
│       ├── main.py                  # Entry point — orchestrates the pipeline
│       ├── transformations.py       # Pure DataFrame → DataFrame functions
│       ├── schemas.py               # Explicit StructType definitions
│       └── hash_client.py           # HTTP client for hashify API + local fallback
├── tests/
│   ├── conftest.py                  # Session-scoped SparkSession fixture
│   ├── test_transformations.py      # Unit tests for each transformation
│   ├── test_hash_client.py          # API mock tests + fallback tests
│   └── test_integration.py          # End-to-end pipeline tests
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── openssl_legacy.cnf               # OpenSSL config to enable MD4
└── README.md
```

## Assumptions & Design Decisions

#### Schema Discrepancies
The table description of Claims defines the schema with the "claim_type" column, whilst it does not exist in the actual data provided, in its place there is the "claim_urgency" column.
Since there is a "claim_type" column already defined for the schema of the output table, we'll maintain the source data's column name.

#### Unknown Claim ID Prefix
The transformation rules for the "claim_type" define:
- RX = Reinsurance
- CL = Coinsurance

The data contains an unmapped prefix of "CX", for unmapped Claim types we'll use a surrogate value of "Unknown".

#### Missing Policyholder
One of the claims references a policyholder key that does not exist ("PH_115"). A left join preserves all claims; the `policyholder_name` for unmatched records is set to "Unknown". In production, orphaned foreign keys would be flagged via a data quality check and routed to a quarantine table for investigation.

#### Region Column Conflict
Both tables have a region column. Since the output definition states "region of the processed claim", we keep the `region` column from the claims data source.

### Hash API Strategy
The necessity of a MD4 hash for each `claim_id` through HTTP calls to the hashify.net API introduces an I/O dependency inside our workflow which required some design and architectural deliberation.

#### The chosen approach
Due to the volume of data in this use-case, we brought the `claim_id` values to the driver (using `.collect()`), hashed via HTTP calls in a simple loop, reusing the same session, created a lookup dataframe with the hashed data and joined it back to the original dataframe.
This was the choice because it keeps all I/O outside our pipeline boundaries, the Spark transformations remain pure, we take DataFrames in and produce DataFrames out, not exposing the DAG to things like network timeout, the API going down or hitting request thresholds.

#### Alternative approaches
**`mapPartitions`**: Using connection pools, we could an HTTP session for each partition and do the hashing across the executors, this is more scalable for larger datasets, but would introduce I/O inside our transformations and make the pipeline harder to test and the issues mentioned above become a concearn.

**`Pre-computed lookup table`**: Hashing all possible `claim_id` values offline and storing them in a persistent lookup table, using it to join at runtime with zero HTTP calls.

**`Local Hashing`**: With the constraint of doing the hashing through a specific API made this not the go-to choice, but in a real scenario, we could apply hashing algorithms locally without external dependencies that introduce I/O to our DAGs. This was actually used as a fallback to the API calls in the hashing algorithm, since MD4 is deterministic we can guarantee both applications produce ar identical.
##### MD4 Hash Algo
MD4 is a deprecated and cryptographically broken hash algorithm. It is implemented here as the spec requires it. OpenSSL 3.x disables MD4 by default; the Docker image and CI pipeline enable it via a custom openssl_legacy.cnf that activates OpenSSL's legacy provider. The local fallback produces identical output to the API, verified by an integration test.


## Dependencies

| Library | Purpose |
|---------|---------|
| pyspark 3.5.8 | DataFrame processing and transformations |
| requests | HTTP calls to the hashify.net API |
| pandas | Single-file CSV output via `toPandas()` |
| pytest | Test framework |
| chispa | PySpark DataFrame test assertions |
| requests-mock | Mock HTTP responses in tests |

## Output
The pipeline produces `data/output/processed_claims.csv` with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| claim_id | String | Unique identifier for the claim |
| policyholder_name | String | Name of the policyholder ("Unknown" if not found) |
| region | String | Region from the claims data |
| claim_type | String | Derived from claim_id prefix: CL=Coinsurance, RX=Reinsurance, other=Unknown |
| claim_priority | String | "Urgent" if claim_amount > $4000, otherwise "Normal" |
| claim_amount | Decimal(12,2) | Amount claimed in USD |
| claim_period | String | Year and month of the claim_date (YYYY-MM) |
| source_system_id | String | Numeric portion extracted from claim_id |
| hash_id | String | MD4 hash of claim_id via hashify.net API |

## What I Would Do Differently at Scale

### Output Data
At scale it is obvious to point that `toPandas()` and writing a csv is not the preffered option. At scale the ideal choice would be using spark's distrubuted nature coupled with an open-source (or managed) file system like DeltaLake or Iceberg to efficiently write and store the data and it's metadata.

### Unknown IDs
At a production workflow we cannot have things mapped to "Unknown" with disregard. In a real use case where we need these dimensions that are still unexistent the go-to solution would be a warning flow to the data-owner and probably someone from the business side who owns the generation of these dimensions to actually create them and the "Unknown" data can be properly mapped.

### Joining
Taking the context of the data, in a bigger scale scenario we would most likely do a broadcast join betweem the `claims` and the `policy_holder` tables,broadcasting the `policy_holder` table since it is the smaller table (slowly changing dimension table), always setting the approriate threshold for the broadcast depending on our cluster size ($) and SLA constraints, resorting back to sort-merge joins if both exceed those.
If both tables are big and they are frequently joined `bucketing` can be a valid strategy to avoid shuffling. But for it to be really worth it it would require an overall data-model analysis so we are not bucketing only for 2 tables and not considering the whole of the model.

### Hashing
At scale the go-to choice would be a local hashing algorithm if we want to do it at runtime. The alternative would be using `mapPartitions` to create our PreHashed lookup table that would be used in the joins. Avoiding I/O in our Spark pipelines.

### Schema Enforcement
In a scale scenario using Medallion architecture we would most likely keep the bronze layer flexible and explicitly enforce the schemas in the silver layer, running some way of schema validation on bronze to detect drastic schema changes in the source. In this project explicit `StructType` schemas on read serve the same purpose, the pipeline fails on type mismatches rather than silently ingesting corrupt data.

### Monitoring
At scale things like the rate of "Unknown" surrogate keys for unmapped dimensions needs to be monitored, this can be done using assertion checks with logs, or using libraries like Great Expectations or DLT's expectations