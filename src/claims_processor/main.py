from pyspark.sql import SparkSession
from claims_processor.schemas import CLAIMS_SCHEMA, POLICYHOLDER_SCHEMA


def main() -> None:
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("ClaimsProcessor") \
        .getOrCreate()

    claims = spark.read.csv("data/input/claims_data.csv", header=True, schema=CLAIMS_SCHEMA)
    policy_holders = spark.read.csv("data/input/policyholder_data.csv", header=True, schema=POLICYHOLDER_SCHEMA)

    claim_sample_id = claims.limit(1).collect()[0].claim_id

    print(claim_sample_id)

    spark.stop()


if __name__ == "__main__":
    main()