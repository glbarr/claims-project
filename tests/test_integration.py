from decimal import Decimal
from claims_processor.schemas import CLAIMS_SCHEMA, POLICYHOLDER_SCHEMA
import claims_processor.transformations as T


class TestFullPipeline:
    """End-to-end integration test using the real data from the challenge."""

    def _build_claims(self, spark):
        return spark.createDataFrame(
            [
                ("CL_472", "PH_101", "East", "High", Decimal("1500.75"), "2023-01-15"),
                ("RX_819", "PH_101", "West", "Low", Decimal("4200.00"), "2023-02-10"),
                ("CX_235", "PH_237", "West", "High", Decimal("4500.50"), "2023-03-05"),
                ("CL_627", "PH_115", "North", "Low", Decimal("2800.25"), "2023-04-20"),
                ("RX_394", "PH_237", "South", "High", Decimal("1800.00"), "2023-05-12"),
                ("CL_153", "PH_152", "South", "Low", Decimal("5100.00"), "2023-06-08"),
            ],
            ["claim_id", "policyholder_id", "region", "claim_urgency", "claim_amount", "claim_date"],
        )

    def _build_policyholders(self, spark):
        return spark.createDataFrame(
            [
                ("PH_101", "John Doe", "East"),
                ("PH_237", "Jane Smith", "West"),
                ("PH_287", "Alice Brown", "North"),
                ("PH_152", "Bob Johnson", "South"),
            ],
            ["policyholder_id", "policyholder_name", "region"],
        )

    def test_pipeline_produces_correct_row_count(self, spark):
        claims = self._build_claims(spark)
        policyholders = self._build_policyholders(spark)

        processed = (
            T.join_claims_and_policy_holders(claims, policyholders)
            .transform(T.get_claim_type)
            .transform(T.get_claim_priority)
            .transform(T.get_claim_period)
            .transform(T.get_source_system_id)
        )

        assert processed.count() == 6

    def test_pipeline_produces_correct_output_schema(self, spark):
        claims = self._build_claims(spark)
        policyholders = self._build_policyholders(spark)

        processed = (
            T.join_claims_and_policy_holders(claims, policyholders)
            .transform(T.get_claim_type)
            .transform(T.get_claim_priority)
            .transform(T.get_claim_period)
            .transform(T.get_source_system_id)
        )

        # Simulate hash join
        hash_df = spark.createDataFrame(
            [("CL_472", "fakehash1"), ("RX_819", "fakehash2"), ("CX_235", "fakehash3"),
            ("CL_627", "fakehash4"), ("RX_394", "fakehash5"), ("CL_153", "fakehash6")],
            ["claim_id", "hash_id"],
        )

        result = (
            processed
            .join(hash_df, on="claim_id", how="left")
            .transform(T.select_final_schema)
        )

        expected_columns = [
            "claim_id", "policyholder_name", "region", "claim_type",
            "claim_priority", "claim_amount", "claim_period",
            "source_system_id", "hash_id",
        ]
        assert result.columns == expected_columns

    def test_missing_policyholder_gets_unknown_name(self, spark):
        claims = self._build_claims(spark)
        policyholders = self._build_policyholders(spark)

        processed = T.join_claims_and_policy_holders(claims, policyholders)

        row = processed.filter("claim_id = 'CL_627'").collect()[0]
        assert row.policyholder_name == "Unknown"

    def test_cx_prefix_gets_unknown_claim_type(self, spark):
        claims = self._build_claims(spark)
        policyholders = self._build_policyholders(spark)

        processed = (
            T.join_claims_and_policy_holders(claims, policyholders)
            .transform(T.get_claim_type)
        )

        row = processed.filter("claim_id = 'CX_235'").collect()[0]
        assert row.claim_type == "Unknown"

    def test_claim_priority_boundary_values(self, spark):
        claims = self._build_claims(spark)
        policyholders = self._build_policyholders(spark)

        processed = (
            T.join_claims_and_policy_holders(claims, policyholders)
            .transform(T.get_claim_priority)
        )

        rows = {row.claim_id: row.claim_priority for row in processed.collect()}

        # > 4000 = Urgent
        assert rows["RX_819"] == "Urgent"    # 4200.00
        assert rows["CX_235"] == "Urgent"    # 4500.50
        assert rows["CL_153"] == "Urgent"    # 5100.00

        # <= 4000 = Normal
        assert rows["CL_472"] == "Normal"    # 1500.75
        assert rows["CL_627"] == "Normal"    # 2800.25
        assert rows["RX_394"] == "Normal"    # 1800.00

    def test_claim_periods_extracted_correctly(self, spark):
        claims = self._build_claims(spark)
        policyholders = self._build_policyholders(spark)

        processed = (
            T.join_claims_and_policy_holders(claims, policyholders)
            .transform(T.get_claim_period)
        )

        rows = {row.claim_id: row.claim_period for row in processed.collect()}

        assert rows["CL_472"] == "2023-01"
        assert rows["RX_819"] == "2023-02"
        assert rows["CX_235"] == "2023-03"
        assert rows["CL_627"] == "2023-04"
        assert rows["RX_394"] == "2023-05"
        assert rows["CL_153"] == "2023-06"

    def test_source_system_ids_extracted_correctly(self, spark):
        claims = self._build_claims(spark)
        policyholders = self._build_policyholders(spark)

        processed = (
            T.join_claims_and_policy_holders(claims, policyholders)
            .transform(T.get_source_system_id)
        )

        rows = {row.claim_id: row.source_system_id for row in processed.collect()}

        assert rows["CL_472"] == "472"
        assert rows["RX_819"] == "819"
        assert rows["CX_235"] == "235"

    def test_no_null_values_in_required_columns(self, spark):
        """All output columns except hash_id should be non-null."""
        claims = self._build_claims(spark)
        policyholders = self._build_policyholders(spark)

        processed = (
            T.join_claims_and_policy_holders(claims, policyholders)
            .transform(T.get_claim_type)
            .transform(T.get_claim_priority)
            .transform(T.get_claim_period)
            .transform(T.get_source_system_id)
        )

        non_null_columns = [
            "claim_id", "policyholder_name", "region",
            "claim_type", "claim_priority", "claim_amount",
            "claim_period", "source_system_id",
        ]

        for col_name in non_null_columns:
            null_count = processed.filter(f"{col_name} is null").count()
            assert null_count == 0, f"Column {col_name} has {null_count} null values"
