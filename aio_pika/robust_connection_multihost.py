from typing import List, Optional, Any
from yarl import URL
from aio_pika.robust_connection import RobustConnection
import asyncio

class RobustConnectionMultiHost(RobustConnection):
    """
    Robust multi-host connection for RabbitMQ.

    Tries connecting to a list of URLs in sequence
    until one connection succeeds.

    Example:

    .. code-block:: python

        conn = RobustConnectionMultiHost([
            "amqp://guest:guest@host1/",
            "amqp://guest:guest@host2/"
        ])
        await conn.connect()

    :param urls: list of broker URLs
    :param default_port: default port if not specified
    :param kwargs: additional parameters for RobustConnection
    """

    def __init__(self, urls: List[str], default_port: int = 5672, **kwargs: Any):
        self.urls = [self._prepare_url(url, default_port) for url in urls]
        self.kwargs = kwargs
        self._current_url: Optional[URL] = None
        self._connect_timeout: Optional[float] = None
        super().__init__(str(self.urls[0]), **kwargs)

    def _prepare_url(self, url: str, default_port: int) -> URL:
        if not url.startswith("amqp://"):
            url = "amqp://" + url
        url_obj = URL(url)
        if url_obj.port is None:
            auth = f"{url_obj.user}:{url_obj.password}@" if url_obj.user else ""
            url_obj = URL(f"amqp://{auth}{url_obj.host}:{default_port}")
        return url_obj

    async def connect(self, timeout: Optional[float] = None) -> None:
        """
        Connects to the first available server.

        :param timeout: connection timeout
        """
        self._connect_timeout = timeout
        if self._current_url:
            try:
                self.url = str(self._current_url)
                await super().connect(timeout=timeout)
                return
            except Exception:
                self._current_url = None
        last_exc = None
        for url in self.urls:
            try:
                self.url = str(url)
                await super().connect(timeout=timeout)
                self._current_url = url
                return
            except Exception as e:
                last_exc = e
        raise last_exc or RuntimeError("All connection attempts failed")

    async def _on_connection_close(self, closing):
        if not self.is_closed:
            await self.reconnect()
        await super()._on_connection_close(closing)

    async def reconnect(self):
        await asyncio.sleep(2)
        await self.connect(timeout=self._connect_timeout)
        await self.reconnect_callbacks()

__all__ = (
    "RobustConnectionMultiHost"
)
