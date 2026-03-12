from pyspark.sql import SparkSession
from claims_processor.schemas import CLAIMS_SCHEMA, POLICYHOLDER_SCHEMA
from claims_processor.hash_client import hash_claim_ids
import claims_processor.transformations as T


def main() -> None:
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("ClaimsProcessor") \
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
        .select(
            F.col(""),
        )
        )
    
    processed_claims.show()

    spark.stop()


if __name__ == "__main__":
    main()