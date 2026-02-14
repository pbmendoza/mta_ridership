import unittest
from unittest.mock import patch

import requests

from scripts.utils.download import count_rows
from scripts.utils.socrata import SocrataRequestError, request_json


class FakeResponse:
    def __init__(
        self,
        *,
        status_code=200,
        payload=None,
        reason="OK",
        headers=None,
        text="",
        json_exc=None,
    ):
        self.status_code = status_code
        self._payload = payload
        self.reason = reason
        self.headers = headers or {}
        self.text = text
        self._json_exc = json_exc

    def json(self):
        if self._json_exc is not None:
            raise self._json_exc
        return self._payload


class FakeSession:
    def __init__(self, sequence):
        self._sequence = list(sequence)
        self.calls = 0

    def get(self, endpoint, params, headers, timeout):  # noqa: ARG002
        self.calls += 1
        if not self._sequence:
            raise RuntimeError("No fake responses left")
        next_item = self._sequence.pop(0)
        if isinstance(next_item, Exception):
            raise next_item
        return next_item


class RequestJsonTests(unittest.TestCase):
    def test_returns_payload_on_first_success(self):
        session = FakeSession([FakeResponse(status_code=200, payload=[{"ridership": 1}])])
        data = request_json(session, "https://example", {"$limit": "1"}, {"Accept": "application/json"})
        self.assertEqual(data, [{"ridership": 1}])
        self.assertEqual(session.calls, 1)

    @patch("scripts.utils.socrata.time.sleep")
    def test_retries_then_succeeds_for_transient_503(self, mocked_sleep):
        session = FakeSession(
            [
                FakeResponse(status_code=503, reason="Service Unavailable", text="temporary"),
                FakeResponse(status_code=200, payload=[{"ok": True}]),
            ]
        )
        data = request_json(
            session,
            "https://example",
            {"$limit": "1"},
            {"Accept": "application/json"},
            max_retries=3,
            base_backoff_seconds=0.01,
            max_backoff_seconds=0.01,
            backoff_jitter_ratio=0.0,
        )
        self.assertEqual(data, [{"ok": True}])
        self.assertEqual(session.calls, 2)
        self.assertEqual(mocked_sleep.call_count, 1)

    @patch("scripts.utils.socrata.time.sleep")
    def test_retries_on_429_with_retry_after(self, mocked_sleep):
        session = FakeSession(
            [
                FakeResponse(
                    status_code=429,
                    reason="Too Many Requests",
                    headers={"Retry-After": "7"},
                    text="slow down",
                ),
                FakeResponse(status_code=200, payload=[{"ok": True}]),
            ]
        )
        data = request_json(session, "https://example", {}, {"Accept": "application/json"}, max_retries=3)
        self.assertEqual(data, [{"ok": True}])
        self.assertEqual(session.calls, 2)
        self.assertEqual(mocked_sleep.call_count, 1)
        self.assertEqual(mocked_sleep.call_args[0][0], 7.0)

    @patch("scripts.utils.socrata.time.sleep")
    def test_fails_fast_on_non_retryable_403(self, mocked_sleep):
        session = FakeSession(
            [
                FakeResponse(status_code=403, reason="Forbidden", text="not allowed"),
                FakeResponse(status_code=200, payload=[{"ok": True}]),
            ]
        )
        with self.assertRaises(SocrataRequestError):
            request_json(session, "https://example", {}, {"Accept": "application/json"}, max_retries=3)
        self.assertEqual(session.calls, 1)
        self.assertEqual(mocked_sleep.call_count, 0)

    def test_raises_on_invalid_json_body(self):
        session = FakeSession(
            [
                FakeResponse(
                    status_code=200,
                    reason="OK",
                    text="<html>bad payload</html>",
                    json_exc=ValueError("invalid json"),
                )
            ]
        )
        with self.assertRaises(SocrataRequestError) as context:
            request_json(session, "https://example", {}, {"Accept": "application/json"})
        self.assertIn("Failed to decode JSON response", str(context.exception))

    def test_raises_on_non_list_payload(self):
        session = FakeSession([FakeResponse(status_code=200, payload={"count": "10"})])
        with self.assertRaises(SocrataRequestError) as context:
            request_json(session, "https://example", {}, {"Accept": "application/json"})
        self.assertIn("Unexpected payload type", str(context.exception))

    @patch("scripts.utils.socrata.time.sleep")
    def test_raises_after_retry_exhaustion_for_503(self, mocked_sleep):
        session = FakeSession(
            [
                FakeResponse(status_code=503, reason="Service Unavailable", text="temporary outage"),
                FakeResponse(status_code=503, reason="Service Unavailable", text="temporary outage"),
                FakeResponse(status_code=503, reason="Service Unavailable", text="temporary outage"),
            ]
        )
        with self.assertRaises(SocrataRequestError):
            request_json(
                session,
                "https://example",
                {"$limit": "1"},
                {"Accept": "application/json"},
                max_retries=3,
                base_backoff_seconds=0.01,
                max_backoff_seconds=0.01,
                backoff_jitter_ratio=0.0,
            )
        self.assertEqual(session.calls, 3)
        self.assertEqual(mocked_sleep.call_count, 2)

    @patch("scripts.utils.download.request_json")
    def test_count_rows_raises_on_malformed_count_payload(self, mocked_request_json):
        mocked_request_json.return_value = [{"unexpected_key": "NaN", "another": "value"}]
        with self.assertRaises(ValueError):
            count_rows(
                session=object(),
                endpoint="https://example",
                headers={"Accept": "application/json"},
                where_clause="1=1",
            )


class RequestJsonNetworkRetryTests(unittest.TestCase):
    @patch("scripts.utils.socrata.time.sleep")
    def test_retries_on_transport_exception_then_succeeds(self, mocked_sleep):
        session = FakeSession(
            [
                requests.Timeout("timeout"),
                FakeResponse(status_code=200, payload=[{"ok": True}]),
            ]
        )
        data = request_json(
            session,
            "https://example",
            {},
            {"Accept": "application/json"},
            max_retries=3,
            base_backoff_seconds=0.01,
            max_backoff_seconds=0.01,
            backoff_jitter_ratio=0.0,
        )
        self.assertEqual(data, [{"ok": True}])
        self.assertEqual(session.calls, 2)
        self.assertEqual(mocked_sleep.call_count, 1)


if __name__ == "__main__":
    unittest.main()
