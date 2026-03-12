# Claims Processing Pipeline

## Overview
<!-- One paragraph: what this does, what tech it uses -->

## Prerequisites
<!-- Docker, Docker Compose, Python version, etc. -->

## How to Run

### With Docker
```bash
# TODO: fill in exact commands
```

### Without Docker
```bash
# TODO: fill in exact commands
```

### Run Tests
```bash
# TODO: fill in exact commands
```

## Project Structure
```

```

## Assumptions && Design Decisions

#### Schema Discrepancies
The table description of of Claims defines the schema with the "claim_type" column, whilst it does not exist in the actual data provided, in it's place there is the "claim_urgency" column.
Since there is a "claim_type" column already defined for the schema of the output table, we'll maintain the source data's column name.


#### Unknown Claim ID Prefix
The transformation rules for the "claim_type" define:
- RX = Reinsurance
- CL = Coinsurance
The data contains an unmapped prefix of "CX", for unmapped Claim types we'll use a surrogate key "Unknown"


#### Missing Policyholder
One of the claims reference a poilicyholder key that does not exist "PH_115", in this case we'll use the same surrogate key strategy as the Unknown Claim ID prefix, and use "Unknown".

#### Region Column Conflict
Both tables have a region column, since the output definitions states _"region of the processed claim"_ in it's definition, we'll keep the "region" column from the claims data source.

### Hash API Strategy
<!--
MD4 hash via external API call. How do you handle:
- Making HTTP calls within a Spark pipeline
- API failures / timeouts
- Scalability considerations
-->

### MD4 Hash Algorithm
<!--
MD4 is deprecated and insecure. Note this. 
Did you implement a fallback? Why or why not?
-->

## Dependencies
<!-- List key libraries and why each is needed -->

## Output
The pipeline produces `output/processed_claims.csv` with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| claim_id | String | |
| policyholder_name | String | |
| region | String | |
| claim_type | String | |
| claim_priority | String | |
| claim_amount | Float | |
| claim_period | String | |
| source_system_id | String | |
| hash_id | String | |

## What I Would Do Differently at Scale
<!--
This is where you show production thinking.
- How would the HTTP calls change with 10M rows?
- Partitioning strategy?
- Monitoring?
- Data quality checks?
-->
