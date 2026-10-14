from __future__ import absolute_import, print_function, unicode_literals

import unittest

from pybase.client import MainClient
from pybase.exceptions import RegionServerException
from pybase.helpers import region_cache_key
from pybase.region.client import Client


def _region_client(host, port):
    """A Client as pybase builds it: __init__ decodes host and port to str."""
    client = Client.__new__(Client)
    client.host = host.decode('utf8') if isinstance(host, bytes) else host
    client.port = port.decode('utf8') if isinstance(port, bytes) else port
    client.secondary = False
    client.regions = []
    client.closed = False
    client.close = lambda: setattr(client, 'closed', True)
    return client


class TestRegionCacheKey(unittest.TestCase):
    """Region servers arrive as bytes but Client stores them decoded."""

    def test_bytes_and_str_spellings_agree(self):
        self.assertEqual(region_cache_key(b'rs1', b'16020'),
                         region_cache_key('rs1', '16020'))

    def test_handler_reaches_purge_for_a_cached_client(self):
        """Covers the handler calling purge at all, not the key spelling."""
        client = _region_client(b'rs1.example', b'16020')
        main = MainClient.__new__(MainClient)
        main.reverse_client_cache = {region_cache_key(b'rs1.example', b'16020'): client}

        purged = []
        main._purge_client = lambda c: purged.append(c)

        error = RegionServerException(region_client=client)
        try:
            error._handle_exception(main)
        except RegionServerException:
            pass

        self.assertEqual(purged, [client],
                         'cached client was not purged, so a retry reuses it')

    def test_purge_removes_the_entry_the_cache_was_stored_under(self):
        """Mixing a str host with a bytes separator raised, so nothing was purged."""
        client = _region_client(b'rs1.example', b'16020')
        main = MainClient.__new__(MainClient)
        main.reverse_client_cache = {region_cache_key(b'rs1.example', b'16020'): client}
        main.region_cache = {}
        main._cache_lock = __import__('threading').Lock()

        main._purge_client(client)

        self.assertEqual(main.reverse_client_cache, {})
        self.assertTrue(client.closed)


if __name__ == '__main__':
    unittest.main()
