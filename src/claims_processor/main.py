from pyspark.sql import SparkSession
from claims_processor.schemas import CLAIMS_SCHEMA, POLICYHOLDER_SCHEMA
from claims_processor.hash_client import hash_md4_via_api, hash_md4_local


def main() -> None:
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("ClaimsProcessor") \
        .getOrCreate()

    claims = spark.read.csv("data/input/claims_data.csv", header=True, schema=CLAIMS_SCHEMA)
    policy_holders = spark.read.csv("data/input/policyholder_data.csv", header=True, schema=POLICYHOLDER_SCHEMA)

    claim_sample_id = claims.limit(1).collect()[0].claim_id
    hash_result = hash_md4_via_api(claim_sample_id)
    local_hash_result = hash_md4_local(claim_sample_id)

    print(f"claim_id: {claim_sample_id} -> hash: {hash_result} -> local hash: {local_hash_result}")
    print(f"HASHES MATCH:{hash_result==local_hash_result}")

    spark.stop()


if __name__ == "__main__":
    main()