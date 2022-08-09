"""
name: group
author：kavinbj
createdAt: 2021/7/15
version: 2.0.0
description:

"""
import aioredis
from weakref import WeakValueDictionary
from .client import MQClient
from .redispool import RedisPool

from .exceptions import (
    AioGroupError
)

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
GroupT = _StringLikeT  # Consumer group id
ConsumerT = _StringLikeT  # Consumer name
StreamIdT = Union[int, _StringLikeT]


class AbsGroupConsumer(MQClient):
    """
     abstract Class, for cache GroupConsumer instance with key
    """
    __consumer_cache = WeakValueDictionary()

    def __new__(
        cls,
        stream_key: KeyT,
        group_name: GroupT,
        consumer_id: ConsumerT,
        redis_name: Optional[str] = None,
        redis_pool: RedisPool = None,
        group=None
    ):
        """
        cache GroupConsumer instance with key
        :param stream_key:  key of stream
        :param group_name:  group name
        :param consumer_id: redis name
        :param redis_name:  redis url
        :param redis_pool:  aioredis.client.Redis instance
        :param group: Group instance
        :return:
        """
        _key = f'_{str(redis_name)}_{str(stream_key)}_{str(group_name)}_{str(consumer_id)}'
        if _key in cls.__consumer_cache:
            return cls.__consumer_cache[_key]
        else:
            _instance = super().__new__(cls)
            cls.__consumer_cache[_key] = _instance
            return _instance


class GroupConsumer(AbsGroupConsumer):
    def __init__(
        self,
        stream_key: KeyT,
        group_name: GroupT,
        consumer_id: ConsumerT,
        redis_name: Optional[str] = None,
        redis_pool: aioredis.client.Redis = None,
        group=None
    ):
        """
        create a GroupConsumer instance
        :param stream_key: key of the stream.
        :param group_name: name of the group.
        :param consumer_id: id of the consumer
        :param redis_pool: base redis pool instance
        """
        if redis_pool is None or not isinstance(redis_pool, aioredis.client.Redis):
            raise AioGroupError('parameter redis_pool is missing or error instance')

        if not redis_name:
            raise AioGroupError('redis name is None')

        if not stream_key:
            raise AioGroupError('stream key is None')

        if not group_name:
            raise AioGroupError('group name is None')

        if not consumer_id:
            raise AioGroupError('consumer id is None')

        if not group:
            raise AioGroupError('group instance is None')

        self.redis_name = redis_name
        self.stream_key = stream_key
        self.group_name = group_name
        self.consumer_id = consumer_id
        self._group = group
        super().__init__(redis_pool=redis_pool)

    def read_messages(
        self,
        streams: Dict[KeyT, StreamIdT],
        count: Optional[int] = None,
        noack: bool = False
    ) -> Awaitable:
        """
        Read from a stream via a consumer group.
        :param streams:  a dict of stream names to stream IDs, where IDs indicate the last ID already seen.
        :param count: if set, only return this many items, beginning with the earliest available
        :param noack: do not add messages to the PEL (Pending Entries List)
        :return:
        """
        return self.redis_pool.xreadgroup(self.group_name, self.consumer_id, streams, count=count, noack=noack)

    def block_read_messages(
        self,
        *stream_key: KeyT,
        block: Optional[int] = None,
        count: Optional[int] = None,
        noack: bool = False
    ) -> Awaitable:
        """
        Block read from a stream via a consumer group.
        :param stream_key: a list of stream key
        :param block: number of milliseconds to wait, if nothing already present.
        :param count: if set, only return this many items, beginning with the earliest available
        :param noack: do not add messages to the PEL (Pending Entries List)
        :return:
        """
        _stream_key = tuple(set((self.stream_key, ) + stream_key))
        _streams = dict(zip(_stream_key, ['>'] * len(_stream_key)))
        return self.redis_pool.xreadgroup(self.group_name, self.consumer_id, _streams,
                                          block=block, count=count, noack=noack)

    def query_pending_messages(
        self,
        min_msg_id: Optional[StreamIdT],
        max_msg_id: Optional[StreamIdT],
        count: Optional[int]
    ) -> Awaitable:
        """
        Returns information about pending messages, in a range.
        :param min_msg_id:  minimum message ID
        :param max_msg_id:  maximum message ID
        :param count:  number of messages to return
        :return:
        for example:
        """
        return self._redis_pool.xpending_range(self.stream_key, self.group_name, min_msg_id, max_msg_id, count,
                                               consumername=self.consumer_id)

    def ack_message(
        self,
        *msg_id: StreamIdT
    ) -> Awaitable:
        """
        Acknowledges the successful processing of one or more messages.
        :param msg_id: message ids to acknowledge.
        :return:
        """
        return self._redis_pool.xack(self.stream_key, self.group_name, *msg_id)
