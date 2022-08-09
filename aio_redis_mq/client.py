"""
name: client
author：kavinbj
createdAt: 2021/7/2
version: 2.0.0
description:

"""
import aioredis
from .redispool import RedisPool

from aioredis.connection import (
    EncodableT,
)

from .exceptions import (
    AioRedisMQException
)

from typing import (
    Optional,
    Awaitable,
    Union,
    TypeVar
)

_StringLikeT = Union[bytes, str, memoryview]
KeyT = _StringLikeT
FieldT = EncodableT
GroupT = _StringLikeT  # Consumer group id
ConsumerT = _StringLikeT  # Consumer name
StreamIdT = Union[int, _StringLikeT]

_MQClient = TypeVar("_MQClient", bound="MQClient")


class MQClient:
    """
      aioRedisMQ client
    """
    def __init__(
        self,
        redis_name: Optional[str] = None,
        redis_url: Optional[str] = None,
        redis_pool: aioredis.client.Redis = None,
        **kwargs
    ):
        """
        basic client of message system, which can manage and query messages
        :param redis_name: name for cache redis client
        :param redis_url: redis server url
        :param redis_pool: aioredis.client.Redis instance, defaults to None
        :param kwargs:
        """
        super().__init__()

        if redis_pool is not None:
            self._redis_pool = redis_pool

        elif redis_name is not None or redis_url is not None:
            self._redis_pool = RedisPool.get_redis_pool(redis_name, redis_url=redis_url, **kwargs)

        else:
            raise AioRedisMQException('bad parameter, check redis_name redis_url or redis_pool.')

    @classmethod
    def create_from_url(
        cls,
        redis_name: Optional[str],
        redis_url: Optional[str] = None,
        **kwargs
    ) -> _MQClient:
        """
        A class method for create MQ Client instance
        :param redis_name: redis server name
        :param redis_url:  redis server url
        :param kwargs:
        :return:
        """
        return cls(redis_name=redis_name, redis_url=redis_url, **kwargs)

    @classmethod
    def create_from_redis_pool(
        cls,
        redis_pool: aioredis.client.Redis = None
    ) -> _MQClient:
        """
        A class method for create MQ Client instance
        :param redis_pool: aioredis.client.Redis instance
        :return:
        """
        if not redis_pool or not isinstance(redis_pool, aioredis.client.Redis):
            raise AioRedisMQException('parameter redis_pool is None or error instance')

        return cls(redis_pool=redis_pool)

    @property
    def redis_pool(self):
        """
        return property redis_pool
        :return:
        """
        return self._redis_pool

    async def check_health(self):
        """
        check redis server health status
        :return:
        """
        return await self._redis_pool.ping()

    def get_stream_length(
        self,
        stream_key: KeyT
    ) -> Awaitable:
        """
        Returns the number of elements in a given stream.
        """
        return self._redis_pool.xlen(stream_key)

    def query_messages(
        self,
        stream_key: KeyT,
        min_id: StreamIdT = "-",
        max_id: StreamIdT = "+",
        count: Optional[int] = None
    ) -> Awaitable:
        """
        Read stream values within an interval.
        :param stream_key:  key of the stream.
        :param min_id: first stream ID. defaults to '-', meaning the earliest available.
        :param max_id: last stream ID. defaults to '+', meaning the latest available.
        :param count: if set, only return this many items, beginning with the earliest available.
        :return:
        """
        return self._redis_pool.xrange(stream_key, min=min_id, max=max_id, count=count)

    def reverse_query_messages(
        self,
        stream_key: KeyT,
        max_id: StreamIdT = "+",
        min_id: StreamIdT = "-",
        count: Optional[int] = None,
    ) -> Awaitable:
        """
        Read stream values within an interval, in reverse order.
        :param stream_key: key of the stream.
        :param max_id: first stream ID. defaults to '+', meaning the latest available.
        :param min_id: last stream ID. defaults to '-', meaning the earliest available.
        :param count: if set, only return this many items, beginning with the latest available.
        :return:
        """
        return self._redis_pool.xrevrange(stream_key, max=max_id, min=min_id, count=count)

    def get_stream_info(
        self,
        stream_key: KeyT
    ) -> Awaitable:
        """
        Returns general information about the stream.
        :param stream_key: key of the stream.
        :return:
        """
        return self._redis_pool.xinfo_stream(stream_key)

    def delete_message(
        self,
        stream_key: KeyT,
        *ids: StreamIdT
    ) -> Awaitable:
        """
        Deletes one or more messages from a stream.
        :param stream_key: key of the stream.
        :param ids: message ids to delete.
        :return:
        """
        return self._redis_pool.xdel(stream_key, *ids)

    def trim_stream(
        self,
        stream_key: KeyT,
        maxlen: int,
        approximate: bool = True
    ) -> Awaitable:
        """
        Trims old messages from a stream.
        :param stream_key: key of the stream.
        :param maxlen: truncate old stream messages beyond this size
        :param approximate: actual stream length may be slightly more than maxlen
        :return:
        """
        return self._redis_pool.xtrim(stream_key, maxlen=maxlen, approximate=approximate)
