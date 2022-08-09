"""
name: redis_pool
author：kavinbj
createdAt: 2021/7/3
version: 2.0.0
description:

"""
import logging
import aioredis
from typing import Optional
from .exceptions import AioRedisMQException, AioRedisMQValueError
from weakref import WeakValueDictionary

logger = logging.getLogger(__name__)


class RedisPool:
    __redis_cache = WeakValueDictionary()

    @classmethod
    def get_redis_pool(
        cls,
        redis_name: Optional[str],
        redis_url: Optional[str] = None,
        decode_responses=True,
        **kwargs
    ):
        """
        Return a Redis client object configured from the given URL
        cache the client instance base on redis_name

        For example:

            redis://[[username]:[password]]@localhost:6379/0
            rediss://[[username]:[password]]@localhost:6379/0
            unix://[[username]:[password]]@/path/to/socket.sock?db=0

        Three URL schemes are supported:

        - `redis://` creates a TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/redis>
        - `rediss://` creates a SSL wrapped TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/rediss>
        - ``unix://``: creates a Unix Domain Socket connection.

        The username, password, hostname, path and all querystring values
        are passed through urllib.parse.unquote in order to replace any
        percent-encoded values with their corresponding characters.

        There are several ways to specify a database number. The first value
        found will be used:
            1. A ``db`` querystring option, e.g. redis://localhost?db=0
            2. If using the redis:// or rediss:// schemes, the path argument
               of the url, e.g. redis://localhost/0
            3. A ``db`` keyword argument to this function.

        If none of these options are specified, the default db=0 is used.

        :param redis_name: cache a aioredis pool instance based on this name
        :param redis_url: aioredis client url
        :param decode_responses: indicates whether to decode responses automatically.
        :param kwargs:refer to: https://aioredis.readthedocs.io/en/latest/api/high-level/#aioredis.client.Redis.from_url
           other keywords argument
           For example:
           db:int = 0,  database number
           max_connections

        :return: aioredis client
        """
        if redis_name is None:
            redis_name = '_local_'

        if redis_name in cls.__redis_cache:
            return cls.__redis_cache[redis_name]
        else:
            if not redis_url:
                raise AioRedisMQException(f'no {redis_name} cache instance, You must specify a redis_url')
            try:
                _redis_pool = aioredis.from_url(redis_url, decode_responses=decode_responses, **kwargs)
            except ValueError:
                raise AioRedisMQValueError(f'create aioredis from_url ={redis_url} error')

            setattr(_redis_pool, '_redis_name', redis_name)
            setattr(_redis_pool, '_redis_url', redis_url)
            cls.__redis_cache[redis_name] = _redis_pool

            return _redis_pool

    @classmethod
    async def check_health(cls, redis_name, **kwargs):
        """
        Ping the Redis server, check the health of the connection with a PING/PONG
        :param redis_name: cache a aioredis pool instance based on this name
        :param kwargs:
        :return:
        """
        redis_pool = cls.get_redis_pool(redis_name, **kwargs)

        try:
            result = await redis_pool.ping()
            logger.info(f'redis {redis_name} connecting {"successful" if result else "error"}.')
            return result
        except aioredis.RedisError:
            raise AioRedisMQException(f'redis {redis_name} connecting aioredis error.')

    @classmethod
    async def init_redis(cls, redis_name, **kwargs):
        """
        initial redis instance
        :param redis_name:
        :param kwargs:
        :return:
        """
        return await cls.check_health(redis_name, **kwargs)

    @classmethod
    async def close_redis(cls, redis_name, **kwargs):
        """
        close
        :param redis_name: cache a aioredis pool instance based on this name
        :param kwargs:
        :return:
        """
        redis_pool = cls.get_redis_pool(redis_name, **kwargs)
        await redis_pool.close()
        cls.__redis_cache.pop(redis_name)
        logger.info(f'redis {redis_name} connection close.')
