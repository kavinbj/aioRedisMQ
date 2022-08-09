"""
name: producer
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
    Awaitable,
    Optional,
    Dict,
    Union
)

_StringLikeT = Union[bytes, str, memoryview]
KeyT = _StringLikeT
FieldT = EncodableT
StreamIdT = Union[int, _StringLikeT]


class MQProducer(MQClient):
    def __init__(
        self,
        stream_key: KeyT,
        redis_name: Optional[str] = None,
        redis_url: Optional[str] = None,
        redis_pool: aioredis.client.Redis = None,
        **kwargs
    ):
        """
         producer client in message queue based on a specific stream key
        :param stream_key: key of stream which is the container of messages
        :param redis_name: name for cache redis client
        :param redis_url: redis server url
        :param redis_pool: aioredis.client.Redis instance, defaults to None
        :param kwargs:
        """
        self.stream_key = stream_key
        super().__init__(redis_name=redis_name, redis_url=redis_url, redis_pool=redis_pool, **kwargs)

    def send_message(
        self,
        message: Dict[FieldT, EncodableT],
        msg_id: StreamIdT = "*",
        maxlen: int = None,
        approximate: bool = True
    ) -> Awaitable:
        """
        send message content to stream which is a message container
        :param message: dict of field/value pairs to insert into the stream
        :param msg_id:  Location to insert this record. By default it is appended.
        :param maxlen: truncate old stream members beyond this size
        :param approximate: actual stream length may be slightly more than maxlen
        :return: msg_id
        """
        return self.redis_pool.xadd(self.stream_key, message, id=msg_id, maxlen=maxlen, approximate=approximate)
