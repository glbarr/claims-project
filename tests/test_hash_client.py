import pytest
import requests
import requests_mock as rm

from claims_processor.hash_client import (
    hash_md4_via_api,
    hash_md4_local,
    hash_claim_ids,
    HASHIFY_BASE_URL,
)


class TestHashMd4ViaApi:

    def test_returns_digest_from_api(self, requests_mock):
        requests_mock.get(
            HASHIFY_BASE_URL,
            json={"Digest": "abc123def456"},
        )

        result = hash_md4_via_api("CL_472")

        assert result == "abc123def456"

    def test_passes_claim_id_as_query_param(self, requests_mock):
        requests_mock.get(
            HASHIFY_BASE_URL,
            json={"Digest": "abc123"},
        )

        hash_md4_via_api("CL_472")

        assert requests_mock.last_request.qs == {"value": ["cl_472"]}

    def test_raises_on_server_error(self, requests_mock):
        requests_mock.get(
            HASHIFY_BASE_URL,
            status_code=500,
        )

        with pytest.raises(requests.exceptions.HTTPError):
            hash_md4_via_api("CL_472", timeout=1)

    def test_raises_on_timeout(self, requests_mock):
        requests_mock.get(
            HASHIFY_BASE_URL,
            exc=requests.exceptions.ConnectTimeout,
        )

        with pytest.raises(requests.exceptions.ConnectTimeout):
            hash_md4_via_api("CL_472", timeout=1)


class TestHashMd4Local:

    def test_returns_hex_string(self):
        result = hash_md4_local("CL_472")
        assert isinstance(result, str)
        assert all(c in "0123456789abcdef" for c in result)

    def test_deterministic(self):
        assert hash_md4_local("CL_472") == hash_md4_local("CL_472")

    def test_different_inputs_different_hashes(self):
        assert hash_md4_local("CL_472") != hash_md4_local("RX_819")


class TestHashClaimIds:

    def test_uses_api_when_available(self, requests_mock):
        requests_mock.get(
            HASHIFY_BASE_URL,
            json={"Digest": "api_hash_value"},
        )

        result = hash_claim_ids(["CL_472"])

        assert result == {"CL_472": "api_hash_value"}

    def test_falls_back_to_local_on_api_failure(self, requests_mock):
        requests_mock.get(
            HASHIFY_BASE_URL,
            exc=requests.exceptions.ConnectionError,
        )

        result = hash_claim_ids(["CL_472"])
        expected_local = hash_md4_local("CL_472")

        assert result == {"CL_472": expected_local}

    def test_hashes_multiple_claim_ids(self, requests_mock):
        requests_mock.get(
            HASHIFY_BASE_URL,
            json={"Digest": "some_hash"},
        )

        result = hash_claim_ids(["CL_472", "RX_819", "CX_235"])

        assert len(result) == 3
        assert all(v == "some_hash" for v in result.values())

    def test_mixed_api_success_and_failure(self, requests_mock):
        """First call succeeds, second fails — should use fallback for second."""
        responses = [
            {"json": {"Digest": "api_hash"}},
            {"exc": requests.exceptions.ConnectionError},
        ]
        requests_mock.get(HASHIFY_BASE_URL, responses)

        result = hash_claim_ids(["CL_472", "RX_819"])

        assert result["CL_472"] == "api_hash"
        assert result["RX_819"] == hash_md4_local("RX_819")

@pytest.mark.integration
class TestHashConsistency:

    def test_api_and_local_produce_same_hash(self):
        """Requires network access. Run with: pytest -m integration"""
        claim_id = "CL_472"
        api_result = hash_md4_via_api(claim_id)
        local_result = hash_md4_local(claim_id)

        assert api_result == local_result