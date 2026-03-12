from decimal import Decimal
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
import pyspark.sql.functions as F

from claims_processor.transformations import (
    join_claims_and_policy_holders,
    get_claim_type,
    get_claim_priority,
    get_claim_period,
    get_source_system_id,
    select_final_schema,
)


class TestJoinClaimsAndPolicyHolders:

    def test_matched_policyholder_joins_name(self, spark):
        claims = spark.createDataFrame(
            [("CL_001", "PH_101", "East", "High", Decimal("1500.75"), "2023-01-15")],
            ["claim_id", "policyholder_id", "region", "claim_urgency", "claim_amount", "claim_date"],
        )
        policyholders = spark.createDataFrame(
            [("PH_101", "John Doe", "East")],
            ["policyholder_id", "policyholder_name", "region"],
        )

        result = join_claims_and_policy_holders(claims, policyholders)

        assert result.collect()[0].policyholder_name == "John Doe"

    def test_unmatched_policyholder_returns_unknown(self, spark):
        claims = spark.createDataFrame(
            [("CL_001", "PH_999", "North", "Low", Decimal("2800.25"), "2023-04-20")],
            ["claim_id", "policyholder_id", "region", "claim_urgency", "claim_amount", "claim_date"],
        )
        policyholders = spark.createDataFrame(
            [("PH_101", "John Doe", "East")],
            ["policyholder_id", "policyholder_name", "region"],
        )

        result = join_claims_and_policy_holders(claims, policyholders)

        assert result.collect()[0].policyholder_name == "Unknown"

    def test_keeps_claims_region_not_policyholder_region(self, spark):
        claims = spark.createDataFrame(
            [("CL_001", "PH_101", "West", "High", Decimal("1500.00"), "2023-01-15")],
            ["claim_id", "policyholder_id", "region", "claim_urgency", "claim_amount", "claim_date"],
        )
        policyholders = spark.createDataFrame(
            [("PH_101", "John Doe", "East")],
            ["policyholder_id", "policyholder_name", "region"],
        )

        result = join_claims_and_policy_holders(claims, policyholders)

        assert result.collect()[0].region == "West"

    def test_preserves_all_claims_on_left_join(self, spark):
        claims = spark.createDataFrame(
            [
                ("CL_001", "PH_101", "East", "High", Decimal("1000.00"), "2023-01-01"),
                ("CL_002", "PH_999", "West", "Low", Decimal("2000.00"), "2023-02-01"),
            ],
            ["claim_id", "policyholder_id", "region", "claim_urgency", "claim_amount", "claim_date"],
        )
        policyholders = spark.createDataFrame(
            [("PH_101", "John Doe", "East")],
            ["policyholder_id", "policyholder_name", "region"],
        )

        result = join_claims_and_policy_holders(claims, policyholders)

        assert result.count() == 2


class TestGetClaimType:

    def test_cl_prefix_maps_to_coinsurance(self, spark):
        df = spark.createDataFrame([("CL_472",)], ["claim_id"])
        result = get_claim_type(df)
        assert result.collect()[0].claim_type == "Coinsurance"

    def test_rx_prefix_maps_to_reinsurance(self, spark):
        df = spark.createDataFrame([("RX_819",)], ["claim_id"])
        result = get_claim_type(df)
        assert result.collect()[0].claim_type == "Reinsurance"

    def test_unknown_prefix_maps_to_unknown(self, spark):
        df = spark.createDataFrame([("CX_235",)], ["claim_id"])
        result = get_claim_type(df)
        assert result.collect()[0].claim_type == "Unknown"

    def test_completely_unknown_prefix(self, spark):
        df = spark.createDataFrame([("ZZ_999",)], ["claim_id"])
        result = get_claim_type(df)
        assert result.collect()[0].claim_type == "Unknown"


class TestGetClaimPriority:

    def test_amount_above_4000_is_urgent(self, spark):
        df = spark.createDataFrame(
            [(Decimal("4500.50"),)], ["claim_amount"]
        )
        result = get_claim_priority(df)
        assert result.collect()[0].claim_priority == "Urgent"

    def test_amount_below_4000_is_normal(self, spark):
        df = spark.createDataFrame(
            [(Decimal("1500.75"),)], ["claim_amount"]
        )
        result = get_claim_priority(df)
        assert result.collect()[0].claim_priority == "Normal"

    def test_amount_exactly_4000_is_normal(self, spark):
        """Spec says > 4000, not >= 4000. Boundary test."""
        df = spark.createDataFrame(
            [(Decimal("4000.00"),)], ["claim_amount"]
        )
        result = get_claim_priority(df)
        assert result.collect()[0].claim_priority == "Normal"


class TestGetClaimPeriod:

    def test_extracts_year_month(self, spark):
        df = spark.createDataFrame([("2023-01-15",)], ["claim_date"])
        result = get_claim_period(df)
        assert result.collect()[0].claim_period == "2023-01"

    def test_different_month(self, spark):
        df = spark.createDataFrame([("2023-12-31",)], ["claim_date"])
        result = get_claim_period(df)
        assert result.collect()[0].claim_period == "2023-12"


class TestGetSourceSystemId:

    def test_extracts_numeric_portion(self, spark):
        df = spark.createDataFrame([("CL_472",)], ["claim_id"])
        result = get_source_system_id(df)
        assert result.collect()[0].source_system_id == "472"

    def test_different_prefix(self, spark):
        df = spark.createDataFrame([("RX_819",)], ["claim_id"])
        result = get_source_system_id(df)
        assert result.collect()[0].source_system_id == "819"


class TestSelectFinalSchema:

    def test_output_has_correct_columns_in_order(self, spark):
        df = spark.createDataFrame(
            [(
                "CL_001", "PH_101", "John Doe", "East", "High",
                Decimal("1500.75"), "2023-01-15",
                "Coinsurance", "Normal", "2023-01", "001", "abc123"
            )],
            [
                "claim_id", "policyholder_id", "policyholder_name", "region",
                "claim_urgency", "claim_amount", "claim_date",
                "claim_type", "claim_priority", "claim_period",
                "source_system_id", "hash_id"
            ],
        )

        result = select_final_schema(df)

        expected_columns = [
            "claim_id", "policyholder_name", "region", "claim_type",
            "claim_priority", "claim_amount", "claim_period",
            "source_system_id", "hash_id"
        ]
        assert result.columns == expected_columns

    def test_drops_intermediate_columns(self, spark):
        df = spark.createDataFrame(
            [(
                "CL_001", "PH_101", "John Doe", "East", "High",
                Decimal("1500.75"), "2023-01-15",
                "Coinsurance", "Normal", "2023-01", "001", "abc123"
            )],
            [
                "claim_id", "policyholder_id", "policyholder_name", "region",
                "claim_urgency", "claim_amount", "claim_date",
                "claim_type", "claim_priority", "claim_period",
                "source_system_id", "hash_id"
            ],
        )

        result = select_final_schema(df)

        assert "policyholder_id" not in result.columns
        assert "claim_urgency" not in result.columns
        assert "claim_date" not in result.columns
