from __future__ import absolute_import, print_function, unicode_literals

import unittest

from pybase.client import MAX_REQUEST_ATTEMPTS, MainClient
from pybase.exceptions import PyBaseException


class RecoverableException(PyBaseException):
    """A PyBaseException that _handle_exception treats as retryable."""

    def _handle_exception(self, main_client, **kwargs):
        return


class TestRequestRetryBounds(unittest.TestCase):
    """Requests must retry a bounded number of times rather than recursing.

    The recursive implementation kept every attempt's frame, row values and
    built request alive, so a row that never succeeded exhausted container
    memory before Python's recursion limit was reached.
    """

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


if __name__ == '__main__':
    unittest.main()
