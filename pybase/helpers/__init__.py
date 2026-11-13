from __future__ import absolute_import, print_function

from . import varint  # noqa


def region_cache_key(host, port):
    """Key for MainClient.reverse_client_cache, as a str.

    Region servers arrive as bytes from protobuf but Client decodes them, so the
    two spellings have to be normalised or a cached client is never found again.
    """
    if isinstance(host, bytes):
        host = host.decode('utf8')
    if isinstance(port, bytes):
        port = port.decode('utf8')
    return host + ':' + port
