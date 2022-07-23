"""
name: test_consumer
author：kavinbj
createdAt: 2021/7/20
version: 2.0.0
description:

"""
import asyncio
import aioredis
import pytest
import time

import sys
sys.path.append("..")
from aio_redis_mq import RedisPool, MQProducer, MQConsumer


@pytest.mark.asyncio
async def test_clear_key(get_redis_url):
    redis_pool = RedisPool.get_redis_pool('_test_local1', redis_url=get_redis_url)
    results = await redis_pool.delete('_test_stream1', '_test_stream2', '_test_stream3')
    assert results >= 0


@pytest.mark.asyncio
async def test_consumer_create(get_redis_url):

    consumer = MQConsumer('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)
    assert isinstance(consumer, MQConsumer)

    redis_pool = consumer._redis_pool
    assert isinstance(redis_pool, aioredis.client.Redis)


@pytest.mark.asyncio
async def test_consumer_read_messages(get_redis_url):
    producer = MQProducer('_test_stream2', redis_name='_test_local1', redis_url=get_redis_url)

    consumer = MQConsumer('_test_stream2', redis_name='_test_local1', redis_url=get_redis_url)

    # produce message, return msg_id
    send_msg_id = await producer.send_message({'msgkey1': '_test_stream2_value1', 'msgkey2': '_test_stream2_value2'})
    # read all message in a stream
    messages = await consumer.read_messages({'_test_stream2': '0'})

    assert messages[0][1][-1] == (send_msg_id, {'msgkey1': '_test_stream2_value1', 'msgkey2': '_test_stream2_value2'})


async def producer_task(producer):
    msg_id_list = []
    for _ in range(0, 5):
        await asyncio.sleep(0.5)
        send_msg_id = await producer.send_message({'msg': f'msg_{_}', 'content': time.strftime("%Y-%m-%d %H:%M:%S")})
        print(f'producer_task time at {time.strftime("%Y-%m-%d %H:%M:%S")}', f'message id={send_msg_id}')
        msg_id_list.append(send_msg_id)

    return msg_id_list


async def consumer_task(consumer: MQConsumer, consumer_index: int):
    msg_id_list = []
    for _ in range(0, 5):
        msg = await consumer.block_read_messages(block=800)
        # print(f'consumer_{consumer_index} block read message', msg)
        if len(msg) > 0 and len(msg[0][1]) > 0:
            msg_id_list.append(msg[0][1][0][0])
    return msg_id_list


@pytest.mark.asyncio
async def test_consumer_block_read_messages(get_redis_url):
    # one producer
    producer = MQProducer('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)

    # three consumer
    consumer1 = MQConsumer('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)
    consumer2 = MQConsumer('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)
    consumer3 = MQConsumer('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)

    msg_ids_list = await asyncio.gather(
        producer_task(producer),
        consumer_task(consumer1, 1),
        consumer_task(consumer2, 2),
        consumer_task(consumer3, 3)
    )
    # Compare produce msg_id_list and 3 consumer msg_id_list
    check_list = [len(set(x)) == 1 for x in list(zip(*msg_ids_list))]

    assert all(check_list) is True




