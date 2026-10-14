from __future__ import absolute_import, print_function, unicode_literals

import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from pybase import exceptions
from pybase.client import MainClient
from pybase.exceptions import RegionServerException
from pybase.helpers import region_cache_key
from pybase.region.client import Client


def _region_client(host, port, with_pool=False):
    client = Client.__new__(Client)
    client.host = host
    client.port = port
    client.secondary = False
    client.regions = []
    client.shutting_down = False
    client.sock_pool = []
    client.thread_pool = ThreadPoolExecutor(max_workers=1) if with_pool else None
    return client


def _main_with(cached):
    main = MainClient.__new__(MainClient)
    main.reverse_client_cache = dict(cached)
    main.region_cache = {}
    main._cache_lock = threading.Lock()
    return main


class TestClientRetirement(unittest.TestCase):

    def test_late_failure_does_not_evict_the_replacement(self):
        """Normalised keys let a retired client's failure match its successor."""
        key = region_cache_key('rs1', '16020')
        retired = _region_client('rs1', '16020')
        replacement = _region_client('rs1', '16020')
        main = _main_with({key: replacement})

        main._purge_client(retired)

        self.assertIs(main.reverse_client_cache.get(key), replacement)

    def test_purging_the_cached_client_still_evicts_it(self):
        key = region_cache_key('rs1', '16020')
        cached = _region_client('rs1', '16020')
        main = _main_with({key: cached})

        main._purge_client(cached)

        self.assertEqual(main.reverse_client_cache, {})

    def test_close_retires_the_executor(self):
        """Closing sockets alone leaves the pool's worker threads running."""
        client = _region_client('rs1', '16020', with_pool=True)
        pool = client.thread_pool

        client.close()

        self.assertTrue(pool._shutdown)

    def test_repeated_retirement_does_not_grow_the_bucket_map(self):
        """The bucket key holds the region client, so entries must be released."""
        exceptions._buckets.clear()
        for _ in range(25):
            client = _region_client('rs1', '16020')
            error = RegionServerException(region_client=client)
            exceptions._let_one_through(error, client)
            exceptions._let_all_through(error, client)
        self.assertEqual(len(exceptions._buckets), 0)


    def test_bucket_round_trip_claims_and_releases_one_semaphore(self):
        """Covers the claim/release round trip. The interleaving that motivated
        making the claim atomic is not reproduced here; see the PR discussion."""
        exceptions._buckets.clear()
        client = _region_client('rs1', '16020')
        first = RegionServerException(region_client=client)
        second = RegionServerException(region_client=client)

        self.assertTrue(exceptions._let_one_through(first, client))
        # Owner finishes and retires the bucket.
        exceptions._let_all_through(first, client)
        # A later caller must claim a live bucket, not the retired semaphore.
        self.assertTrue(exceptions._let_one_through(second, client))
        claimed = exceptions._buckets[(second.__class__.__name__, client)]
        exceptions._let_all_through(second, client)
        self.assertEqual(claimed._value, 1, 'released a different semaphore')

    def test_submit_after_retirement_is_a_region_failure(self):
        """RuntimeError from a retired pool would bypass the retry handlers."""
        client = _region_client('rs1', '16020', with_pool=True)
        client.call_lock = threading.Lock()
        client.call_id = 0
        client.thread_pool_timeout = 5

        class FakePb(object):
            def SerializeToString(self):
                return b'x'

        class FakeRequest(object):
            type = 'Get'
            pb = FakePb()

        client.close()

        with self.assertRaises(RegionServerException):
            client._send_request(FakeRequest())


if __name__ == '__main__':
    unittest.main()
