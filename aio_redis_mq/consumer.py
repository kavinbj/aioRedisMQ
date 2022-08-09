"""
name: consumer
author：kavinbj
createdAt: 2021/7/12
version: 2.0.0
description:

"""
from .client import MQClient
import aioredis
from aioredis.connection import (
    EncodableT,
)

from typing import (
    Optional,
    Awaitable,
    Dict,
    Union
)

_StringLikeT = Union[bytes, str, memoryview]
KeyT = _StringLikeT
FieldT = EncodableT
StreamIdT = Union[int, _StringLikeT]


class MQConsumer(MQClient):
    def __init__(
        self,
        stream_key: KeyT,
        redis_name: Optional[str] = None,
        redis_url: Optional[str] = None,
        redis_pool: aioredis.client.Redis = None,
        **kwargs
    ):
        """
        consumer client in message queue based on a specific stream key
        :param stream_key:
        :param redis_name: name for cache redis client
        :param redis_url: redis server url
        :param redis_pool: aioredis.client.Redis instance, defaults to None
        :param kwargs:
        """
        self.stream_key = stream_key
        super().__init__(redis_name=redis_name, redis_url=redis_url, redis_pool=redis_pool, **kwargs)

    def read_messages(
        self,
        streams: Dict[KeyT, StreamIdT],
        count: Optional[int] = None
    ) -> Awaitable:
        """
        consumer specific method, read messages from streams as message containers
        :param streams: a dict of stream keys to stream IDs, where
               IDs indicate the last ID already seen.
        :param count: if set, only return this many items, beginning with the
           earliest available.
        :return:
        """
        return self.redis_pool.xread(streams, count=count)

    def block_read_messages(
        self,
        *stream_key: KeyT,
        count: Optional[int] = None,
        block: Optional[int] = None,
    ) -> Awaitable:
        """
        Block and monitor multiple streams for new data.
        :param stream_key: key of the stream.
        :param count: if set, only return this many items, beginning with the
           earliest available.
        :param block: number of milliseconds to wait, if nothing already present.
        :return:
        """
        # add current key to stream_key tuple
        _stream_key = tuple(set((self.stream_key, ) + stream_key))
        # set the corresponding ID to $，and can block new messages
        _streams = dict(zip(_stream_key, ['$'] * len(_stream_key)))
        return self.redis_pool.xread(_streams, count=count, block=block)
