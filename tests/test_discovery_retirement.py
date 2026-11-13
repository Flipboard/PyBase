from __future__ import absolute_import, print_function, unicode_literals

import threading
import unittest

from intervaltree import IntervalTree

import pybase.region.client as region
from pybase.client import MainClient
from pybase.exceptions import RegionServerException
from pybase.helpers import region_cache_key


class FakeRegion(object):
    def __init__(self, table, start_key, stop_key):
        self.table = table
        self.start_key = start_key
        self.stop_key = stop_key
        self.region_client = None

    def __repr__(self):
        return 'FakeRegion(%r,%r,%r)' % (self.table, self.start_key, self.stop_key)


def _region_client(host, port):
    client = region.Client.__new__(region.Client)
    client.host = host
    client.port = port
    client.secondary = False
    client.regions = []
    client.shutting_down = False
    client.sock_pool = []
    client.thread_pool = None
    return client


def _main_with(cached):
    main = MainClient.__new__(MainClient)
    main.reverse_client_cache = dict(cached)
    main.region_cache = IntervalTree()
    main._cache_lock = threading.Lock()
    main.pool_size = 1
    main.secondary = False
    main.call_timeout = 1
    return main


def _cached_clients(main):
    return [iv.data.region_client for iv in main.region_cache]


class TestDiscoveryAndRetirement(unittest.TestCase):
    """Discovery must never cache a region against a client that was retired."""

    def setUp(self):
        self.key = region_cache_key(b'rs1', b'16020')
        self.connects = []
        self._real_new_client = region.NewClient

        def fake_new_client(host, port, pool_size, secondary=False, call_timeout=60):
            self.connects.append((host, port))
            hook = getattr(self, 'during_connect', None)
            if hook is not None:
                hook()
            return _region_client('rs1', '16020')

        region.NewClient = fake_new_client

    def tearDown(self):
        region.NewClient = self._real_new_client

    def _discover(self, main, start=b'a', stop=b'b'):
        reg = FakeRegion(b't', start, stop)
        main._attach_region_to_live_client(reg, b'rs1', b'16020', self.key)
        return reg

    def test_live_cached_client_is_reused_without_connecting(self):
        cached = _region_client('rs1', '16020')
        main = _main_with({self.key: cached})

        reg = self._discover(main)

        self.assertIs(reg.region_client, cached)
        self.assertEqual(self.connects, [])
        self.assertEqual(cached.regions, [reg])

    def test_retired_cached_client_is_replaced(self):
        """_close_old_regions closes a client but leaves its cache entry behind."""
        stale = _region_client('rs1', '16020')
        stale.shutting_down = True
        main = _main_with({self.key: stale})

        reg = self._discover(main)

        self.assertIsNot(reg.region_client, stale)
        self.assertFalse(reg.region_client.shutting_down)
        self.assertIs(main.reverse_client_cache[self.key], reg.region_client)
        self.assertEqual(len(self.connects), 1)

    def test_retirement_during_connect_does_not_lose_the_new_client(self):
        """The cached client is purged while we are connecting its replacement."""
        old = _region_client('rs1', '16020')
        old.shutting_down = True
        main = _main_with({self.key: old})
        self.during_connect = lambda: main._purge_client(old)

        reg = self._discover(main)

        self.assertIs(main.reverse_client_cache[self.key], reg.region_client)
        self.assertFalse(reg.region_client.shutting_down)

    def test_competing_discovery_during_connect_uses_the_winner(self):
        """Two threads miss at once: the second to insert adopts the first's client."""
        main = _main_with({})
        winner = _region_client('rs1', '16020')

        def insert_winner():
            main.reverse_client_cache[self.key] = winner

        self.during_connect = insert_winner

        reg = self._discover(main)

        self.assertIs(reg.region_client, winner)
        self.assertIs(main.reverse_client_cache[self.key], winner)
        self.assertEqual(len(self.connects), 1)

    def test_rediscovering_an_overlapping_region_keeps_the_client_open(self):
        """A split or a stale entry overlaps the cached region. Closing the
        shared client for that left every other cached region on it dead."""
        cached = _region_client('rs1', '16020')
        main = _main_with({self.key: cached})
        parent = self._discover(main, b'a', b'z')
        other = self._discover(main, b'z', b'')

        child = self._discover(main, b'a', b'm')

        self.assertFalse(cached.shutting_down)
        self.assertIs(child.region_client, cached)
        self.assertNotIn(parent, cached.regions)
        self.assertEqual(sorted(cached.regions, key=repr), sorted([other, child], key=repr))
        self.assertEqual(sorted(_cached_clients(main), key=id), sorted([cached, cached], key=id))

    def test_displaced_client_with_no_regions_left_is_retired(self):
        """The region moved to another server and it was the old client's only
        cached region: keeping that client would leak its sockets and threads."""
        old_key = region_cache_key(b'rs0', b'16020')
        old = _region_client('rs0', '16020')
        main = _main_with({old_key: old})
        moved = FakeRegion(b't', b'a', b'z')
        moved.region_client = old
        main._add_to_region_cache(moved)

        reg = self._discover(main, b'a', b'z')

        self.assertTrue(old.shutting_down)
        self.assertNotIn(old_key, main.reverse_client_cache)
        self.assertIs(main.reverse_client_cache[self.key], reg.region_client)
        self.assertEqual(_cached_clients(main), [reg.region_client])

    def test_displaced_client_with_other_regions_stays_open(self):
        old_key = region_cache_key(b'rs0', b'16020')
        old = _region_client('rs0', '16020')
        main = _main_with({old_key: old})
        for start, stop in ((b'a', b'm'), (b'm', b'z')):
            r = FakeRegion(b't', start, stop)
            r.region_client = old
            main._add_to_region_cache(r)

        self._discover(main, b'a', b'm')

        self.assertFalse(old.shutting_down)
        self.assertIs(main.reverse_client_cache[old_key], old)
        self.assertEqual(len(old.regions), 1)

    def test_failed_connect_raises_region_server_exception(self):
        region.NewClient = lambda *a, **k: None
        main = _main_with({})

        with self.assertRaises(RegionServerException):
            self._discover(main)
        self.assertEqual(main.reverse_client_cache, {})

    def test_purge_cannot_land_between_selection_and_insertion(self):
        """A purge issued while discovery holds the lock waits, then finds the
        region it has to evict, so no cached region ever points at a closed
        client. Before the fix, selection and insertion were unlocked."""
        cached = _region_client('rs1', '16020')
        main = _main_with({self.key: cached})
        purge_started = threading.Event()
        purge_done = threading.Event()

        real_add = main._add_to_region_cache_locked

        def add_then_let_purge_run(new_region):
            # Holding _cache_lock here; the purge below must block until we return.
            real_add(new_region)
            threading.Thread(target=lambda: (purge_started.set(),
                                             main._purge_client(cached),
                                             purge_done.set())).start()
            self.assertTrue(purge_started.wait(1))
            self.assertFalse(purge_done.wait(0.2), 'purge ran inside the locked section')

        main._add_to_region_cache_locked = add_then_let_purge_run

        reg = self._discover(main)
        self.assertTrue(purge_done.wait(2))

        # The purge saw the region and evicted it with its client.
        self.assertEqual(_cached_clients(main), [])
        self.assertNotIn(self.key, main.reverse_client_cache)
        self.assertTrue(cached.shutting_down)
        self.assertIs(reg.region_client, cached)

    def test_concurrent_discovery_and_retirement_keeps_the_cache_consistent(self):
        main = _main_with({})
        errors = []
        stop = threading.Event()

        def discover_loop(n):
            i = 0
            while not stop.is_set():
                start = ('%d-%d' % (n, i % 7)).encode('utf8')
                try:
                    self._discover(main, start, start + b'z')
                except Exception as e:  # pragma: no cover - reported below
                    errors.append(e)
                i += 1

        def retire_loop():
            while not stop.is_set():
                client = main.reverse_client_cache.get(self.key)
                if client is not None:
                    main._purge_client(client)

        threads = [threading.Thread(target=discover_loop, args=(n,)) for n in range(4)]
        threads.append(threading.Thread(target=retire_loop))
        for t in threads:
            t.start()
        threading.Event().wait(0.5)
        stop.set()
        for t in threads:
            t.join(5)

        self.assertEqual(errors, [])
        with main._cache_lock:
            live = main.reverse_client_cache.get(self.key)
            for client in _cached_clients(main):
                self.assertFalse(client.shutting_down, 'cached region points at a closed client')
                self.assertIs(client, live, 'cached region points at an evicted client')


if __name__ == '__main__':
    unittest.main()
