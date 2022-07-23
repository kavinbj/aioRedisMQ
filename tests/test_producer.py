"""
name: test_producer
author：kavinbj
createdAt: 2021/7/20
version: 2.0.0
description:

"""
import aioredis
import pytest
import sys
sys.path.append("..")
from aio_redis_mq import RedisPool, MQProducer


@pytest.mark.asyncio
async def test_clear_key(get_redis_url):
    redis_pool = RedisPool.get_redis_pool('_test_local1', redis_url=get_redis_url)
    results = await redis_pool.delete('_test_stream1', '_test_stream2', '_test_stream3')
    assert results >= 0


@pytest.mark.asyncio
async def test_producer_create(get_redis_url):

    producer = MQProducer('_test_stream2', redis_name='_test_local2', redis_url=get_redis_url)
    assert isinstance(producer, MQProducer)

    redis_pool = producer._redis_pool
    assert isinstance(redis_pool, aioredis.client.Redis)


@pytest.mark.asyncio
async def test_producer_query_stream_messages(get_redis_url):
    producer = MQProducer('_test_stream2', redis_name='_test_local2', redis_url=get_redis_url)

    # produce message, return msg_id
    send_msg_id = await producer.send_message({'msgkey1': '_test_stream2_value1', 'msgkey2': '_test_stream2_value2'})

    stream_lenght = await producer.get_stream_length(producer.stream_key)

    stream_info = await producer.get_stream_info(producer.stream_key)

    assert send_msg_id == stream_info.get('last-generated-id')

    assert stream_lenght == stream_info.get('length')

    first_message_info = await producer.query_messages(producer.stream_key, min_id='-', max_id='+', count=1)

    last_message_info = await producer.reverse_query_messages(producer.stream_key, max_id='+', min_id='-', count=1)

    assert first_message_info[0] == stream_info.get('first-entry')

    assert last_message_info[0] == stream_info.get('last-entry')



