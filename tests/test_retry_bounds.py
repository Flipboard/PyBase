from __future__ import absolute_import, print_function, unicode_literals

import gc
import unittest
import weakref

from pybase.client import MAX_REQUEST_ATTEMPTS, MainClient
from pybase.exceptions import PyBaseException


class RecoverableException(PyBaseException):
    """A PyBaseException that _handle_exception treats as retryable."""

    def _handle_exception(self, main_client, **kwargs):
        return


class TestRequestRetryBounds(unittest.TestCase):
    """Retries must stay bounded and must not accumulate memory per attempt."""

    def setUp(self):
        self.client = MainClient.__new__(MainClient)
        self.attempts = []

        def always_fails(table, key):
            self.attempts.append(key)
            raise RecoverableException('region unavailable')

        self.client._find_hosting_region = always_fails

    def test_mutate_stops_after_max_attempts(self):
        with self.assertRaises(RecoverableException):
            self.client._mutate('table', 'key', {'cf': {'col': 'val'}}, None)
        self.assertEqual(len(self.attempts), MAX_REQUEST_ATTEMPTS)

    def test_get_stops_after_max_attempts(self):
        with self.assertRaises(RecoverableException):
            self.client.get('table', 'key')
        self.assertEqual(len(self.attempts), MAX_REQUEST_ATTEMPTS)

    def test_mutate_returns_once_a_retry_succeeds(self):
        calls = []

        class FakeRegionClient(object):
            def _send_request(self, rq):
                return None

        class FakeRegion(object):
            region_client = FakeRegionClient()

        def fails_once(table, key):
            calls.append(key)
            if len(calls) == 1:
                raise RecoverableException('region moved')
            return FakeRegion()

        self.client._find_hosting_region = fails_once
        self.client._mutate('table', 'key', {}, lambda region, key, values: None)
        self.assertEqual(len(calls), 2)

    def test_successful_retry_does_not_retain_payload(self):
        """A saved exception retains the values via its traceback's frame."""
        class Sentinel(object):
            pass

        class FakeRegionClient(object):
            def _send_request(self, rq):
                return None

        class FakeRegion(object):
            region_client = FakeRegionClient()

        calls = []

        def fails_once(table, key):
            calls.append(key)
            if len(calls) == 1:
                raise RecoverableException('region moved')
            return FakeRegion()

        self.client._find_hosting_region = fails_once
        sentinel = Sentinel()
        values = {'payload': sentinel}
        payload_ref = weakref.ref(sentinel)

        gc.disable()
        try:
            self.client._mutate('table', 'key', values, lambda region, key, vals: None)
            del values, sentinel
            self.assertIsNone(payload_ref())
        finally:
            gc.enable()

    def test_retrying_does_not_accumulate_memory(self):
        """Flat memory per attempt is what makes the bound safe to raise."""
        built = []

        class FakeRequest(object):
            def __init__(self):
                self.payload = bytearray(1_000_000)

        def rq_type(region, key, values):
            request = FakeRequest()
            built.append(weakref.ref(request))
            return request

        class FakeRegionClient(object):
            def _send_request(self, rq):
                raise RecoverableException('broken pipe')

        class FakeRegion(object):
            region_client = FakeRegionClient()

        self.client._find_hosting_region = lambda table, key: FakeRegion()

        gc.disable()
        try:
            with self.assertRaises(RecoverableException):
                self.client._mutate('table', 'key', {'cf': {}}, rq_type)
            self.assertEqual(len(built), MAX_REQUEST_ATTEMPTS)
            self.assertEqual([r for r in built if r() is not None], [])
        finally:
            gc.enable()

    def test_bound_leaves_room_for_a_region_reconnect(self):
        """10 sat below a reconnect's attempts and failed live writes."""
        self.assertGreaterEqual(MAX_REQUEST_ATTEMPTS, 50)


if __name__ == '__main__':
    unittest.main()
