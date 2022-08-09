"""
name: group_manager
author：kavinbj
createdAt: 2021/7/15
version: 2.0.0
description:

"""
from weakref import WeakValueDictionary
from .client import MQClient
from .group import Group
import aioredis
import logging

from .exceptions import (
    AioGroupError,
    AioGroupCreateError
)


from aioredis.connection import (
    EncodableT,
)

from typing import (
    Awaitable,
    Optional,
    Union
)

logger = logging.getLogger(__name__)

_StringLikeT = Union[bytes, str, memoryview]
KeyT = _StringLikeT
FieldT = EncodableT
GroupT = _StringLikeT  # Consumer group name
ConsumerT = _StringLikeT  # Consumer id
StreamIdT = Union[int, _StringLikeT]


class Manager(MQClient):
    """
    abstract Class, for cache Manager instance with key
    """
    __manager_cache = WeakValueDictionary()

    def __new__(
        cls,
        stream_key: KeyT,
        redis_name: str = None,
        **kwargs
    ):
        """

        :param stream_key:
        :param redis_name:
        :param kwargs:
        :return:
        """
        _key = f'_{str(redis_name)}_{str(stream_key)}'
        if _key in cls.__manager_cache:
            return cls.__manager_cache[_key]
        else:
            _instance = super().__new__(cls)
            cls.__manager_cache[_key] = _instance

            return _instance


class GroupManager(Manager):

    def __init__(
        self,
        stream_key: KeyT,
        redis_name: str = None,
        redis_url: Optional[str] = None,
        **kwargs
    ):
        """
        GroupManager create method
        :param stream_key: key of the stream.
        :param redis_name:
        :param kwargs:
        """
        if not stream_key:
            raise AioGroupError('stream key is None')

        if not redis_name:
            raise AioGroupError('redis name is None')

        super().__init__(redis_name=redis_name, redis_url=redis_url, **kwargs)

        self._groups = {}
        self.redis_name = redis_name
        self.stream_key = stream_key

    async def create_group(
        self,
        group_name: GroupT,
        msg_id: StreamIdT = "$",
        mkstream: bool = True
    ):
        """
        Create a new group consumer associated with a stream
        :param group_name:  name of the consumer group
        :param msg_id:  ID of the last item in the stream to consider already delivered.
        :param mkstream: a boolean indicating whether to create new stream
        :return:
        """
        if not group_name:
            raise AioGroupError('group name is None')

        if group_name in self._groups:
            return self._groups[group_name]

        # create a new group command
        try:
            result = await self._redis_pool.xgroup_create(self.stream_key, group_name, id=msg_id, mkstream=mkstream)
            logger.info(f'stream={self.stream_key} group_name={group_name} xgroup_create '
                        f'{ "successful" if result else "failed"}')

        except aioredis.exceptions.RedisError as e:
            if str(e) == 'BUSYGROUP Consumer Group name already exists':
                logger.debug(f'{e} group_name={group_name}')
            else:
                raise AioGroupCreateError(str(e))

        # create a group instance
        _group = Group(self.stream_key, group_name, redis_name=self.redis_name, redis_pool=self._redis_pool)
        self._groups.update({group_name: _group})
        return _group

    def destroy_group(
        self,
        group_name: GroupT
    ) -> Awaitable:
        """
        Destroy a consumer group
        :param group_name: name of the consumer group.
        :return:
        """
        self._groups.pop(group_name)
        return self._redis_pool.xgroup_destroy(self.stream_key, group_name)

    async def get_groups_info(self):
        """
        Returns general information about the consumer groups of the stream.
        :return:
        """
        return await self._redis_pool.xinfo_groups(self.stream_key)
