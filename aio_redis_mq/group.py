"""
name: group
author：kavinbj
createdAt: 2021/7/17
version: 2.0.0
description:

"""
import aioredis
from weakref import WeakValueDictionary
from .client import MQClient
from .redispool import RedisPool
from .group_consumer import GroupConsumer
import logging

from .exceptions import (
    AioGroupError
)

from aioredis.connection import (
    EncodableT,
)

from typing import (
    List,
    Tuple,
    Optional,
    Awaitable,
    Union
)

_StringLikeT = Union[bytes, str, memoryview]
KeyT = _StringLikeT
FieldT = EncodableT
GroupT = _StringLikeT  # Consumer group id
ConsumerT = _StringLikeT  # Consumer name
StreamIdT = Union[int, _StringLikeT]

logger = logging.getLogger(__name__)


class AbsGroup(MQClient):
    """
     abstract Class, for cache Group instance with key
    """
    __group_cache = WeakValueDictionary()

    def __new__(
        cls,
        stream_key: KeyT,
        group_name: GroupT,
        redis_name: Optional[str] = None,
        redis_url: Optional[str] = None,
        redis_pool: aioredis.client.Redis = None,
        **kwargs
    ):
        """
        cache Group instance with key
        :param stream_key: key of stream
        :param group_name: group name
        :param redis_name: redis name
        :param redis_url:  redis url
        :param redis_pool: aioredis.client.Redis instance
        :param kwargs:
        :return:
        """
        _key = f'_{str(redis_name)}_{str(stream_key)}_{str(group_name)}'
        if _key in cls.__group_cache:
            return cls.__group_cache[_key]
        else:
            _instance = super().__new__(cls)
            cls.__group_cache[_key] = _instance
            return _instance


class Group(AbsGroup):

    def __init__(
        self,
        stream_key: KeyT,
        group_name: GroupT,
        redis_name: Optional[str] = None,
        redis_pool: RedisPool = None,
        redis_url: Optional[str] = None,
        **kwargs
    ):
        """
        create a Group instance
        :param stream_key: key of the stream.
        :param group_name: name of the group.
        :param redis_pool: aioredis.client.Redis instance
        :param redis_name: redis name if create redis pool instance,
        :param redis_url: redis url if create redis pool instance,
        """
        if redis_pool is None or not isinstance(redis_pool, aioredis.client.Redis):
            if redis_name is None or redis_url is None:
                raise AioGroupError('parameter missing redis_pool or missing redis_name redis_url')
            else:
                redis_pool = RedisPool.get_redis_pool(redis_name, redis_url=redis_url, **kwargs)

        if not redis_name:
            raise AioGroupError('redis name is None')

        if not stream_key:
            raise AioGroupError('stream key is None')

        if not group_name:
            raise AioGroupError('group name is None')

        self._consumers = WeakValueDictionary()
        self.redis_name = redis_name
        self.stream_key = stream_key
        self.group_name = group_name
        super().__init__(redis_pool=redis_pool)

    async def create_consumer(self, consumer_id: ConsumerT):
        """
        create a consumer instance in group
        :param consumer_id: id of consumer.
        :return:
        """
        _consumer = GroupConsumer(self.stream_key, self.group_name, consumer_id, redis_pool=self._redis_pool,
                                  redis_name=self.redis_name, group=self)
        self._consumers.update({consumer_id: _consumer})
        return _consumer

    def delete_consumer(self, consumer_id: ConsumerT):
        """
        Remove a specific consumer from a consumer group.
        Returns the number of pending messages that the consumer had before it was deleted
        :param consumer_id: id of the consumer.
        :return:
        """
        self._consumers.pop(consumer_id, None)
        return self._redis_pool.xgroup_delconsumer(self.stream_key, self.group_name, consumer_id)

    def set_msg_id(
        self,
        msg_id: StreamIdT
    ) -> Awaitable:
        """
        Set the consumer group last delivered ID to something else.
        :param msg_id: ID of the last item in the stream to consider already delivered
        :return:
        """
        return self._redis_pool.xgroup_setid(self.stream_key, self.group_name, msg_id)

    async def get_groups_info(self):
        """
        Returns general information about the consumer groups of the stream.
        :return:
        """
        return await self._redis_pool.xinfo_groups(self.stream_key)

    def get_consumers_info(self) -> Awaitable:
        """
        Returns general information about the consumers in the group.
        only return consumer which has read message
        :return:
        """
        return self._redis_pool.xinfo_consumers(self.stream_key, self.group_name)

    def get_pending_info(self) -> Awaitable:
        """
        Returns information about pending messages of a group.
        :return:
        for example:
        {'pending': 11, 'min': '1658029206803-0', 'max': '1658029236490-0',
        'consumers': [{'name': 'consumer1', 'pending': 6}, {'name': 'consumer2', 'pending': 5}]}
        """
        return self._redis_pool.xpending(self.stream_key, self.group_name)

    def query_pending_messages(
        self,
        min_msg_id: Optional[StreamIdT],
        max_msg_id: Optional[StreamIdT],
        count: Optional[int],
        consumer_id: Optional[ConsumerT] = None
    ) -> Awaitable:
        """
        Returns information about pending messages, in a range.
        :param min_msg_id:  minimum message ID
        :param max_msg_id:  maximum message ID
        :param count:  number of messages to return
        :param consumer_id: id of a consumer to filter by (optional)
        :return:
        for example:
        """
        return self._redis_pool.xpending_range(self.stream_key, self.group_name, min_msg_id, max_msg_id, count,
                                               consumername=consumer_id)

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

    def claim_message(
        self,
        consumer_id: ConsumerT,
        min_idle_time: int,
        msg_ids: Union[List[StreamIdT], Tuple[StreamIdT]],
        idle: Optional[int] = None,
        time: Optional[int] = None,
        retrycount: Optional[int] = None,
        force: bool = False,
        justid: bool = False
    ) -> Awaitable:
        """
        Changes the ownership of a pending message.
        In the context of a stream consumer group, this command changes the ownership of a pending message,
        so that the new owner is the consumer specified as the command argument.
        :param consumer_id: name of a consumer that claims the message.
        :param min_idle_time: filter messages that were idle less than this amount of milliseconds
        :param msg_ids: non-empty list or tuple of message IDs to claim
        :param idle: Set the idle time (last time it was delivered) of the message in ms
        :param time: optional integer. This is the same as idle but instead of a relative amount of milliseconds,
                     it sets the idle time to a specific Unix time (in milliseconds).
        :param retrycount:  optional integer. set the retry counter to the specified value.
                           This counter is incremented every time a message is delivered again.
        :param force: optional boolean, false by default. Creates the pending message entry in the PEL
                      even if certain specified IDs are not already in the PEL assigned to a different client.
        :param justid: optional boolean, false by default. Return just an array of IDs of messages successfully
                      claimed, without returning the actual message
        :return:
        """
        return self._redis_pool.xclaim(self.stream_key, self.group_name, consumer_id, min_idle_time, msg_ids,
                                       idle=idle, time=time, retrycount=retrycount, force=force, justid=justid)
