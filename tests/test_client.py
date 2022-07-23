"""
name: test_client
author：felix
createdAt: 2022/7/20
version: 1.0.0
description:

"""
import aioredis
import pytest
import sys
sys.path.append("..")
from aio_redis_mq import RedisPool, MQClient, MQProducer, exceptions


@pytest.mark.asyncio
async def test_clear_key(get_redis_url):
    redis_pool = RedisPool.get_redis_pool('_test_local1', redis_url=get_redis_url)
    results = await redis_pool.delete('_test_stream1', '_test_stream2', '_test_stream3')
    assert results >= 0


@pytest.mark.asyncio
async def test_client_create(get_redis_url):
    # same redis_name, same instance. RedisPool cache the instance via redis_name.
    client = MQClient(redis_name='_test_local1', redis_url=get_redis_url)
    assert isinstance(client, MQClient)

    redis_pool = client._redis_pool
    assert isinstance(redis_pool, aioredis.client.Redis)

    with pytest.raises(exceptions.AioRedisMQException) as e:
        client = MQClient(redis_name=None)
        assert isinstance(client, MQClient)
    # exec_msg = e.value.args[0]
    assert e.type is exceptions.AioRedisMQException


@pytest.mark.asyncio
async def test_client_create_from_url(get_redis_url):
    client = MQClient.create_from_url('_test_local1', redis_url=get_redis_url)
    assert isinstance(client, MQClient)


@pytest.mark.asyncio
async def test_create_from_redis_pool(get_redis_url):
    redis_pool = RedisPool.get_redis_pool('_test_local1', redis_url=get_redis_url)
    client = MQClient.create_from_redis_pool(redis_pool=redis_pool)
    assert isinstance(client, MQClient)

    with pytest.raises(exceptions.AioRedisMQException) as e:
        client = MQClient.create_from_redis_pool(redis_pool=None)
        assert isinstance(client, MQClient)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioRedisMQException
    assert exec_msg == 'parameter redis_pool is None or error instance'


@pytest.mark.asyncio
async def test_client_check_health(get_redis_url):
    redis_pool = RedisPool.get_redis_pool('_test_local1', redis_url=get_redis_url)

    client = MQClient.create_from_redis_pool(redis_pool=redis_pool)

    is_health = await client.check_health()

    assert is_health is True


@pytest.mark.asyncio
async def test_client_query_messages(get_redis_url):

    producer = MQProducer('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)
    # produce message, return msg_id
    send_msg_id = await producer.send_message({'msgkey1': '_test_stream1_value1', 'msgkey2': '_test_stream1_value2'})
    # get MQClient instance
    client = MQClient(redis_name='_test_local1', redis_url=get_redis_url)

    # get stream length
    stream_length = await client.get_stream_length('_test_stream1')

    # get stream info
    stream_info = await client.get_stream_info('_test_stream1')

    # print('stream_info', stream_info)

    assert stream_info.get('length') == stream_length

    assert stream_info.get('last-generated-id') == send_msg_id

    # get first_message_info
    first_message_info = await client.query_messages('_test_stream1', count=1)
    # get last_message_info
    last_message_info = await client.reverse_query_messages('_test_stream1', count=1)

    assert first_message_info[0] == stream_info.get('first-entry')

    assert last_message_info[0] == stream_info.get('last-entry')


@pytest.mark.asyncio
async def test_client_delete_message(get_redis_url):
    producer = MQProducer('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)
    # produce message, return msg_id
    send_msg_id = await producer.send_message({'msgkey1': '_test_stream1_value1', 'msgkey2': '_test_stream1_value2'})

    # get MQClient instance
    client = MQClient(redis_name='_test_local1', redis_url=get_redis_url)

    # get stream length
    stream_length_before_delete = await client.get_stream_length('_test_stream1')

    delete_result = await client.delete_message('_test_stream1', send_msg_id)

    assert delete_result == 1

    # get stream length
    stream_length_after_delete = await client.get_stream_length('_test_stream1')

    assert stream_length_before_delete == stream_length_after_delete + 1


@pytest.mark.asyncio
async def test_client_trim_stream(get_redis_url):
    producer = MQProducer('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)

    # produce message, return msg_id
    await producer.send_message({'msgkey1': '_test_stream1_value1', 'msgkey2': '_test_stream1_value2'})

    # get MQClient instance
    client = MQClient(redis_name='_test_local2', redis_url=get_redis_url)

    # get stream length
    stream_length_before_trim = await client.get_stream_length('_test_stream1')

    trim_result = await client.trim_stream('_test_stream1', stream_length_before_trim - 1, approximate=False)

    assert trim_result == 1

    stream_length_after_trim = await client.get_stream_length('_test_stream1')

    assert stream_length_before_trim == stream_length_after_trim + 1


