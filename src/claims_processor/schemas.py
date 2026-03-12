from pyspark.sql.types import StructType, StructField, StringType, DecimalType

CLAIMS_SCHEMA = StructType([
    StructField("claim_id", StringType()),
    StructField("policyholder_id", StringType()),
    StructField("region", StringType()),
    StructField("claim_urgency", StringType()),
    StructField("claim_amount", DecimalType(12,2)),
    StructField("claim_date", StringType())
        ]
)

POLICYHOLDER_SCHEMA = StructType([
    StructField("policyholder_id", StringType()),
    StructField("policyholder_name", StringType()),
    StructField("region", StringType())
])