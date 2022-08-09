"""
name: test_group
author：kavinbj
createdAt: 2021/7/21
version: 2.0.0
description:

"""

import asyncio
import pytest
import time

# import sys
# sys.path.append("..")
from aio_redis_mq import RedisPool, GroupManager, Group, MQProducer, exceptions, GroupConsumer


@pytest.mark.asyncio
async def test_clear_key(get_redis_url):
    redis_pool = RedisPool.get_redis_pool('_test_local1', redis_url=get_redis_url)
    results = await redis_pool.delete('_test_stream1', '_test_stream2', '_test_stream3')
    assert results >= 0


def test_group_create_miss_redis_pool():
    with pytest.raises(exceptions.AioGroupError) as e:
        group = Group('_test_stream1', '_group_name', redis_name=None, redis_pool=None)
        assert isinstance(group, Group)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioGroupError
    assert exec_msg == 'parameter missing redis_pool or missing redis_name redis_url'


def test_group_create_miss_redis_name(get_redis_url):
    redis_pool = RedisPool.get_redis_pool('_test_local1', redis_url=get_redis_url)
    with pytest.raises(exceptions.AioGroupError) as e:
        group = Group('_test_stream1', '_group_name', redis_name=None, redis_url=get_redis_url, redis_pool=redis_pool)
        assert isinstance(group, Group)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioGroupError
    assert exec_msg == 'redis name is None'


def test_group_create_miss_stream_key(get_redis_url):
    with pytest.raises(exceptions.AioGroupError) as e:
        group = Group('', '_group_name', redis_name='_test_local1', redis_url=get_redis_url)
        assert isinstance(group, Group)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioGroupError
    assert exec_msg == 'stream key is None'


def test_group_create_miss_group_name(get_redis_url):
    with pytest.raises(exceptions.AioGroupError) as e:
        group = Group('_test_stream1', '', redis_name='_test_local1', redis_url=get_redis_url)
        assert isinstance(group, Group)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioGroupError
    assert exec_msg == 'group name is None'


@pytest.mark.asyncio
async def test_group_create_instance(get_redis_url):

    group_manager = GroupManager('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)
    assert isinstance(group_manager, GroupManager)

    group1 = await group_manager.create_group('_group_name1')

    consumer1 = await group1.create_consumer('_consumer_id')
    cache_consumer1 = await group1.create_consumer('_consumer_id')

    assert consumer1 is cache_consumer1

    group2 = await group_manager.create_group('_group_name2')

    consumer2 = await group1.create_consumer('_consumer_id')
    cache_consumer2 = await group1.create_consumer('_consumer_id')

    assert group2 is not group1

    assert consumer2 is cache_consumer2


@pytest.mark.asyncio
async def test_group_get_groups_info(get_redis_url):

    group_manager = GroupManager('_test_stream2', redis_name='_test_local1', redis_url=get_redis_url)

    group = await group_manager.create_group('_group_name')
    assert group.group_name == '_group_name'

    groups_info = await group_manager.get_groups_info()

    assert len(groups_info) == 1
    assert groups_info[0]['name'] == group.group_name


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
async def test_group_create_consumer(get_redis_url):
    producer = MQProducer('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)

    group_manager = GroupManager('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)

    group = await group_manager.create_group('_group_name')
    assert isinstance(group, Group)

    consumer1 = await group.create_consumer('_consumer1')
    consumer2 = await group.create_consumer('_consumer2')

    producer_ids_list, consumer1_ids_list, consumer2_ids_list = await asyncio.gather(
        producer_task(producer),
        consumer_task(consumer1),
        consumer_task(consumer2)
    )

    consumer_ids_list = sorted(consumer1_ids_list + consumer2_ids_list)

    # Compare produce msg_id_list and consumer msg_id_list
    check_list = [len(set(x)) == 1 for x in list(zip(producer_ids_list, consumer_ids_list))]
    assert all(check_list) is True


@pytest.mark.asyncio
async def test_group_get_info(get_redis_url):
    group_manager = GroupManager('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)
    group = await group_manager.create_group('_group_name')

    stream_length = await group.get_stream_length('_test_stream1')
    stream_messages = await group.query_messages('_test_stream1')
    assert stream_length == len(stream_messages)

    group_info = await group.get_groups_info()
    consumers_info = await group.get_consumers_info()
    pending_info = await group.get_pending_info()

    assert group_info[0]['pending'] == sum([x['pending'] for x in consumers_info])
    assert group_info[0]['pending'] == pending_info['pending']
    assert group_info[0]['last-delivered-id'] == pending_info['max']


@pytest.mark.asyncio
async def test_group_query_pending_messages(get_redis_url):
    group_manager = GroupManager('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)
    group = await group_manager.create_group('_group_name')
    consumer1 = await group.create_consumer('_consumer1')
    assert isinstance(consumer1, GroupConsumer)

    pending_info = await group.get_pending_info()
    pending_messages = await group.query_pending_messages(pending_info['min'], pending_info['max'],
                                                          pending_info['pending'])

    assert len(pending_messages) == pending_info['pending']
    assert pending_messages[0]['message_id'] == pending_info['min']
    assert pending_messages[-1]['message_id'] == pending_info['max']


@pytest.mark.asyncio
async def test_group_set_msg_id(get_redis_url):
    group_manager = GroupManager('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)

    group = await group_manager.create_group('_group_name')

    pending_info = await group.get_pending_info()
    pending_messages = await group.query_pending_messages(pending_info['min'], pending_info['max'],
                                                          pending_info['pending'])

    new_last_id = pending_messages[-2]['message_id']
    set_msg_id_result = await group.set_msg_id(new_last_id)

    assert set_msg_id_result is True

    group_info = await group.get_groups_info()
    assert new_last_id == group_info[0]['last-delivered-id']


@pytest.mark.asyncio
async def test_group_claim_message(get_redis_url):
    group_manager = GroupManager('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)
    group = await group_manager.create_group('_group_name')

    pending_info = await group.get_pending_info()
    # consumer1 pending messgae info
    consumer1_pending_messages_before_claim = await group.query_pending_messages(
        pending_info['min'],
        pending_info['max'],
        pending_info['pending'],
        consumer_id='_consumer1'
    )

    assert len(consumer1_pending_messages_before_claim) > 0

    # choose first message to claim, change the ownership of the message
    claim_msg_id = consumer1_pending_messages_before_claim[0]['message_id']
    time_since_delivered = consumer1_pending_messages_before_claim[0]['time_since_delivered']

    claim_success_ids = await group.claim_message('_consumer2', time_since_delivered, [claim_msg_id],
                                                  idle=0, justid=True)
    assert claim_success_ids[0] == claim_msg_id

    claim_messages = await group.query_pending_messages(claim_msg_id, claim_msg_id, 1)
    assert len(claim_messages) == 1
    assert claim_messages[0]['consumer'] == '_consumer2'


@pytest.mark.asyncio
async def test_group_ack_message(get_redis_url):
    group_manager = GroupManager('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)
    group = await group_manager.create_group('_group_name')

    pending_info_before_ack = await group.get_pending_info()
    pending_messages_before_ack = await group.query_pending_messages(pending_info_before_ack['min'],
                                                                     pending_info_before_ack['max'],
                                                                     pending_info_before_ack['pending'])

    ack_msg_ids = tuple([x['message_id'] for x in pending_messages_before_ack[0:2]])
    ack_msg_num = await group.ack_message(*ack_msg_ids)

    assert ack_msg_num == len(ack_msg_ids)

    pending_info_after_ack = await group.get_pending_info()
    pending_messages_after_ack = await group.query_pending_messages(pending_info_after_ack['min'],
                                                                    pending_info_after_ack['max'],
                                                                    pending_info_after_ack['pending'])

    assert pending_info_after_ack['pending'] == pending_info_before_ack['pending'] - ack_msg_num
    assert pending_info_after_ack['pending'] == len(pending_messages_after_ack)
    assert pending_messages_after_ack[0]['message_id'] == pending_info_after_ack['min']
    assert pending_messages_after_ack[-1]['message_id'] == pending_info_after_ack['max']


@pytest.mark.asyncio
async def test_group_delete_consumer(get_redis_url):
    group_manager = GroupManager('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)

    group = await group_manager.create_group('_group_name')
    consumer1 = await group.create_consumer('_consumer1')
    assert isinstance(consumer1, GroupConsumer)

    consumers_info_before_delete = await group.get_consumers_info()

    consumer1_info = list(filter(lambda x: x['name'] == '_consumer1', consumers_info_before_delete))

    assert len(consumers_info_before_delete) == 2

    pending_msg_num = await group.delete_consumer('_consumer1')

    assert pending_msg_num == consumer1_info[0]['pending']

    consumers_info_after_delete = await group.get_consumers_info()

    assert len(consumers_info_before_delete) == len(consumers_info_after_delete) + 1
