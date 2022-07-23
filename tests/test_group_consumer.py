"""
name: test_group_consumer
author：felix
createdAt: 2022/7/21
version: 1.0.0
description:

"""
import asyncio
import pytest
import time

import sys
sys.path.append("..")
from aio_redis_mq import RedisPool, GroupManager, MQProducer, exceptions, GroupConsumer


@pytest.mark.asyncio
async def test_clear_key(get_redis_url):
    redis_pool = RedisPool.get_redis_pool('_test_local1', redis_url=get_redis_url)
    results = await redis_pool.delete('_test_stream1', '_test_stream2', '_test_stream3')
    assert results >= 0


@pytest.mark.asyncio
async def test_group_consumer_create(get_redis_url):
    group_manager = GroupManager('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)

    group = await group_manager.create_group('_group_name')
    consumer = await group.create_consumer('_consumer1')
    consumer_cache = await group.create_consumer('_consumer1')
    assert consumer is consumer_cache


@pytest.mark.asyncio
async def test_group_consumer_instance(get_redis_url):
    group_manager = GroupManager('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)
    group_instance = await group_manager.create_group('_group_name')

    with pytest.raises(exceptions.AioGroupError) as e:
        consumer = GroupConsumer('_test_stream1', '_group_name', '_consumer1', redis_name=None,
                                 redis_pool=None, group=group_instance)
        assert isinstance(consumer, GroupConsumer)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioGroupError
    assert exec_msg == 'parameter redis_pool is missing or error instance'

    redis_pool = RedisPool.get_redis_pool('_test_local1', redis_url=get_redis_url)
    with pytest.raises(exceptions.AioGroupError) as e:
        consumer = GroupConsumer('_test_stream1', '_group_name', '_consumer1', redis_name=None,
                                 redis_pool=redis_pool, group=group_instance)
        assert isinstance(consumer, GroupConsumer)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioGroupError
    assert exec_msg == 'redis name is None'

    with pytest.raises(exceptions.AioGroupError) as e:
        consumer = GroupConsumer('', '_group_name', '_consumer1', redis_name='_test_local1',
                                 redis_pool=redis_pool, group=group_instance)
        assert isinstance(consumer, GroupConsumer)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioGroupError
    assert exec_msg == 'stream key is None'

    with pytest.raises(exceptions.AioGroupError) as e:
        consumer = GroupConsumer('_test_stream1', '', '_consumer1', redis_name='_test_local1',
                                 redis_pool=redis_pool, group=group_instance)
        assert isinstance(consumer, GroupConsumer)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioGroupError
    assert exec_msg == 'group name is None'


    with pytest.raises(exceptions.AioGroupError) as e:
        consumer = GroupConsumer('_test_stream1', '_group_name', '', redis_name='_test_local1',
                                 redis_pool=redis_pool, group=group_instance)
        assert isinstance(consumer, GroupConsumer)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioGroupError
    assert exec_msg == 'consumer id is None'

    with pytest.raises(exceptions.AioGroupError) as e:
        consumer = GroupConsumer('_test_stream1', '_group_name', '_consumer1', redis_name='_test_local1',
                                 redis_pool=redis_pool, group=None)
        assert isinstance(consumer, GroupConsumer)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioGroupError
    assert exec_msg == 'group instance is None'



async def producer_task(producer):
    msg_id_list = []
    for _ in range(0, 5):
        await asyncio.sleep(0.3)
        send_msg_id = await producer.send_message({'msg': f'msg_{_}', 'content': time.strftime("%Y-%m-%d %H:%M:%S")})
        print(f'producer_task time at {time.strftime("%Y-%m-%d %H:%M:%S")}', f'message id={send_msg_id}')
        msg_id_list.append(send_msg_id)

    return msg_id_list


async def consumer_task(consumer: GroupConsumer):
    msg_id_list = []
    for _ in range(0, 5):
        msg = await consumer.block_read_messages(block=500)
        # print(f'consumer_{consumer.consumer_id} block read message', msg)
        if len(msg) > 0 and len(msg[0][1]) > 0:
            msg_id_list.append(msg[0][1][0][0])
    return msg_id_list


@pytest.mark.asyncio
async def test_group_multi_group(get_redis_url):
    producer = MQProducer('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)

    group_manager = GroupManager('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)

    group1 = await group_manager.create_group('_group_name1')
    consumer1 = await group1.create_consumer('_consumer1')
    consumer2 = await group1.create_consumer('_consumer2')

    group2 = await group_manager.create_group('_group_name2')
    consumer3 = await group2.create_consumer('_consumer3')
    consumer4 = await group2.create_consumer('_consumer4')

    producer_ids_list, \
    consumer1_ids_list, consumer2_ids_list, \
    consumer3_ids_list, consumer4_ids_list = await asyncio.gather(
        producer_task(producer),
        consumer_task(consumer1),
        consumer_task(consumer2),
        consumer_task(consumer3),
        consumer_task(consumer4),
    )

    group1_ids_list = sorted(consumer1_ids_list + consumer2_ids_list)
    group2_ids_list = sorted(consumer3_ids_list + consumer4_ids_list)

    # Compare producer msg_id_list and group1 msg_id_list and group2 msg_id_list
    check_list = [len(set(x)) == 1 for x in list(zip(producer_ids_list, group1_ids_list, group2_ids_list))]
    assert all(check_list) is True


@pytest.mark.asyncio
async def test_group_read_messages(get_redis_url):
    group_manager = GroupManager('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)

    group1 = await group_manager.create_group('_group_name1')
    consumer1 = await group1.create_consumer('_consumer1')

    pending_info = await group1.get_pending_info()

    consumer1_pending_msgs_before_read = await consumer1.query_pending_messages(
        pending_info['min'],
        pending_info['max'],
        pending_info['pending']
    )

    # read message from 0
    consumer1_msgs = await consumer1.read_messages({'_test_stream1': 0}, count=pending_info['pending'])

    consumer1_pending_msgs_after_read = await consumer1.query_pending_messages(
        pending_info['min'],
        pending_info['max'],
        pending_info['pending']
    )

    assert len(consumer1_msgs[0][1]) == len(consumer1_pending_msgs_before_read)

    msg_times_delivered_before_read = [x['times_delivered'] + 1 for x in consumer1_pending_msgs_before_read]
    msg_times_delivered_after_read = [x['times_delivered'] for x in consumer1_pending_msgs_after_read]

    assert msg_times_delivered_before_read == msg_times_delivered_after_read


@pytest.mark.asyncio
async def test_group_ack_messages(get_redis_url):
    group_manager = GroupManager('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)

    group1 = await group_manager.create_group('_group_name1')
    consumer1 = await group1.create_consumer('_consumer1')

    consumer1_pending_messages_before_ack = await consumer1.query_pending_messages('-', '+', 100)

    assert len(consumer1_pending_messages_before_ack) > 0

    ack_msg_id = consumer1_pending_messages_before_ack[0]['message_id']

    ack_msg_num = await consumer1.ack_message(ack_msg_id)

    assert ack_msg_num == 1

    consumer1_pending_messages_after_ack = await consumer1.query_pending_messages('-', '+', 100)

    assert len(consumer1_pending_messages_before_ack) == len(consumer1_pending_messages_after_ack) + 1

