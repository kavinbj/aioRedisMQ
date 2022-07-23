"""
name: test_redispool
author：kavinbj
createdAt: 2021/7/20
version: 2.0.0
description:

"""
import aioredis
import pytest
import sys
sys.path.append("..")
from aio_redis_mq import RedisPool, exceptions


@pytest.mark.asyncio
async def test_redis_pool(get_redis_url):
    # same redis_name, same instance. RedisPool cache the instance via redis_name.
    redis_pool1 = RedisPool.get_redis_pool('_test_local1', redis_url=get_redis_url)
    redis_pool2 = RedisPool.get_redis_pool('_test_local1')
    assert redis_pool1 is redis_pool2


@pytest.mark.asyncio
async def test_redis_name(get_redis_url):
    redis_pool = RedisPool.get_redis_pool(None, redis_url=get_redis_url)
    assert redis_pool._redis_name == '_local_'


@pytest.mark.asyncio
async def test_redis_url():
    error_redis_url = 'error_redis_url'
    with pytest.raises(exceptions.AioRedisMQValueError) as e:
        redis_pool = RedisPool.get_redis_pool('_error_local1', redis_url=error_redis_url)
        assert redis_pool._redis_name == '_error_local1'
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioRedisMQValueError
    assert exec_msg == f'create aioredis from_url ={error_redis_url} error'


@pytest.mark.asyncio
async def test_redis_operation(get_redis_url):
    redis_pool = RedisPool.get_redis_pool('_test_local1', redis_url=get_redis_url)

    # basic operation
    await redis_pool.set('test_key', 'test_value')
    result1 = await redis_pool.get('test_key')
    assert result1 == 'test_value'

    async with redis_pool as r:
        result2 = await r.get('test_key')
        assert result1 == result2


@pytest.mark.asyncio
async def test_redis_check_health(get_redis_url):
    is_health = await RedisPool.check_health('_test_local1', redis_url=get_redis_url)
    assert is_health is True

    error_redis_url = 'redis://error_name:error_pass@localhost/1000'

    redis_name = '_error_name'
    with pytest.raises(exceptions.AioRedisMQException) as e:
        s_health = await RedisPool.check_health(redis_name, redis_url=error_redis_url)
        print('s_health', s_health)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioRedisMQException
    assert exec_msg == f'redis {redis_name} connecting aioredis error.'


@pytest.mark.asyncio
async def test_redis_init_redis(get_redis_url):
    is_health = await RedisPool.init_redis('_test_local1', redis_url=get_redis_url)
    assert is_health is True


@pytest.mark.asyncio
async def test_redis_close_redis(get_redis_url):
    await RedisPool.close_redis('_test_local1', redis_url=get_redis_url)
    with pytest.raises(exceptions.AioRedisMQException) as e:
        redis_pool = RedisPool.get_redis_pool('_test_local1')
        assert isinstance(redis_pool, aioredis.client.Redis)
    exec_msg = e.value.args[0]
    assert exec_msg == 'no _test_local1 cache instance, You must specify a redis_url'


