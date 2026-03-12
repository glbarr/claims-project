from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def join_claims_and_policy_holders(claims: DataFrame, policy_holders: DataFrame) -> DataFrame:
    return (
        claims.alias("cl")
        .join(
            policy_holders.alias("ph"),
            F.col("cl.policyholder_id")==F.col("ph.policyholder_id"),
            "left_outer"
    )
    .select(
        F.col("cl.claim_id"),
        F.col("cl.policyholder_id"),
        F.coalesce(F.col("ph.policyholder_name"), F.lit("Unknown")).alias("policyholder_name"),
        F.col("cl.region"),
        F.col("cl.claim_urgency"),
        F.col("cl.claim_amount"),
        F.col("cl.claim_date"),
    ))

def get_source_system_id(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "source_system_id",
        F.split(F.col("claim_id"), "_")[1]
        )
    

def get_claim_type(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "claim_type",
        F.when(
            F.split(F.col("claim_id"), "_")[0] == "CL", "Coinsurance"
        ).when(
            F.split(F.col("claim_id"), "_")[0] == "RX", "Reinsurance"
        ).otherwise("Unknown")
    )

def get_claim_period(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "claim_period",
        F.date_format(F.to_date(F.col("claim_date"), "yyyy-MM-dd"), "yyyy-MM")
    )

def get_claim_priority(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "claim_priority",
        F.when(
            F.col("claim_amount") > 4000, "Urgent"
        ).otherwise("Normal")
    )

def select_final_schema(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("claim_id"),
        F.col("policyholder_name"),
        F.col("region"),
        F.col("claim_type"),
        F.col("claim_priority"),
        F.col("claim_amount"),
        F.col("claim_period"),
        F.col("source_system_id"),
        F.col("hash_id")
    )