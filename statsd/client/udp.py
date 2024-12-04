import socket

from .base import StatsClientBase, PipelineBase


class Pipeline(PipelineBase):

    def __init__(self, client):
        super().__init__(client)
        self._maxudpsize = client._maxudpsize

    def _send(self):
        data = self._stats.popleft()
        while self._stats:
            # Use popleft to preserve the order of the stats.
            stat = self._stats.popleft()
            if len(stat) + len(data) + 1 >= self._maxudpsize:
                self._client._after(data)
                data = stat
            else:
                data += '\n' + stat
        self._client._after(data)


class StatsClient(StatsClientBase):
    """A client for statsd."""

    def __init__(self, host='localhost', port=8125, prefix=None,
                 maxudpsize=512, ipv6=False, ignore_socket_errors=False):
        """Create a new client."""
        self._host = host
        self._port = port
        self._ipv6 = ipv6
        self._prefix = prefix
        self._maxudpsize = maxudpsize
        self._addr = None
        self._sock = None
        self._ignore_socket_errors = ignore_socket_errors
        self._ready = False
        self._open()

    @property
    def is_ready(self):
        """Return True if the client is ready to send data."""
        return self._ready

    def _open(self):
        fam = socket.AF_INET6 if self._ipv6 else socket.AF_INET

        try:
            family, _, _, _, addr = socket.getaddrinfo(
                self._host, self._port, fam, socket.SOCK_DGRAM)[0]
            self._addr = addr
            self._sock = socket.socket(family, socket.SOCK_DGRAM)
            self._prefix = self._prefix
            self._maxudpsize = self._maxudpsize
            self._ready = True
        except (socket.gaierror, OSError):
            if not self._ignore_socket_errors:
                raise
            self._ready = False

    def _send(self, data):
        """Send data to statsd."""
        try:
            self._sock.sendto(data.encode('ascii'), self._addr)
        except (OSError, RuntimeError):
            # No time for love, Dr. Jones!
            pass

    def reset(self):
        self.close()
        self._open()

    def close(self):
        if self._sock and hasattr(self._sock, 'close'):
            self._sock.close()
        self._sock = None

    def pipeline(self):
        return Pipeline(self)
