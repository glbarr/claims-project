from pyspark.sql import SparkSession
from claims_processor.schemas import CLAIMS_SCHEMA, POLICYHOLDER_SCHEMA
from claims_processor.hash_client import hash_claim_ids
import claims_processor.transformations as T


def main() -> None:
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("ClaimsProcessor") \
        .config("spark.sql.shuffle.partitions", "2")\
        .getOrCreate()

    claims = spark.read.csv("data/input/claims_data.csv", header=True, schema=CLAIMS_SCHEMA)
    policy_holders = spark.read.csv("data/input/policyholder_data.csv", header=True, schema=POLICYHOLDER_SCHEMA)

    raw_processed_claims = T.join_claims_and_policy_holders(
        claims=claims,
        policy_holders=policy_holders
    )

    processed_claims = (
        raw_processed_claims
        .transform(T.get_claim_period)
        .transform(T.get_source_system_id)
        .transform(T.get_claim_type)
        .transform(T.get_claim_priority)
        )
    
    claim_ids = [row.claim_id for row in processed_claims.select("claim_id").collect()]
    hashes = hash_claim_ids(claim_ids)
    hash_df = spark.createDataFrame(
        [(k,v) for k, v in hashes.items()],
        ["claim_id", "hash_id"] 
    )

    result = (
        processed_claims
        .join(hash_df, on="claim_id", how="left")
        .transform(T.select_final_schema)
    )
    result.toPandas().to_csv("data/output/processed_claims.csv", index=False)

    result.show()

    spark.stop()


if __name__ == "__main__":
    main()