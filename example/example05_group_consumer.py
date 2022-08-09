"""
name: example05_group_consumer
author：kavinbj
createdAt: 2021/7/18
version: 2.0.0
description:

Here we use a block read message API and do not detect pending messages or handle idle messages
You need to implement the processing strategy of idle messages (e.g: claim idle messages to other consumer)
and the reading of pending messages by yourself when the server starts.
"""
import asyncio
import time
# import sys
# sys.path.append("..")
from aio_redis_mq import MQProducer, GroupManager, Group, GroupConsumer

_redis_url = 'redis://root:xxxxx@localhost/1'


async def producer_task(producer):
    for _ in range(0, 10):
        await asyncio.sleep(1)
        print(f'-------------------------------------{_}-------------------------------------')
        send_msg_id = await producer.send_message({'msg': f'msg_{_}', 'content': time.strftime("%Y-%m-%d %H:%M:%S")})
        print(f'group_producer send_message time at {time.strftime("%Y-%m-%d %H:%M:%S")}', f'message id={send_msg_id}')


async def consumer_task(consumer: GroupConsumer):
    for _ in range(0, 10):
        # Here we use a low-level read message API and do not detect pending messages or handle idle messages
        msg = await consumer.block_read_messages(count=1, block=1500)
        await asyncio.sleep(0.05)
        print(f'group_consumer {consumer.consumer_id} group={consumer.group_name} block read message', msg)
        if len(msg) > 0 and len(msg[0][1]) > 0:
            msg_id = msg[0][1][0][0]
            ack_result = await consumer.ack_message(msg_id)
            print(f'group_consumer {consumer.consumer_id} group={consumer.group_name} ack message id='
                  f'{msg_id} {"successful" if ack_result else "failed"}.')


# show info
async def show_groups_infor(group: Group):
    print(f'-----------------------------{group.group_name}---------- groups info ------------------------------------')
    group_info = await group.get_groups_info()
    print(f'group name: {group.group_name} groups info : {group_info}')
    print(f'-----------------------------{group.group_name}--------- consumer info -----------------------------------')
    consumer_info = await group.get_consumers_info()
    print(f'group name: {group.group_name} consumer info : {consumer_info}')
    print(f'-----------------------------{group.group_name}-------- pending info -------------------------------------')
    pending_info = await group.get_pending_info()
    print(f'group name: {group.group_name} pending info : {pending_info}')


async def main():
    # create one producer
    producer = MQProducer('group_stream1', redis_name='_group_redis_', redis_url=_redis_url)

    # create group manager , via same stream key, same redis_name
    group_manager = GroupManager('group_stream1', redis_name='_group_redis_', redis_url=_redis_url)

    # create first group
    group1 = await group_manager.create_group('group1')
    # create two consumers in the same group
    consumer1 = await group1.create_consumer('consumer1')
    consumer2 = await group1.create_consumer('consumer2')

    # create second group
    group2 = await group_manager.create_group('group2')
    # create three consumers in the same group
    consumer3 = await group2.create_consumer('consumer3')
    consumer4 = await group2.create_consumer('consumer4')
    consumer5 = await group2.create_consumer('consumer5')

    await asyncio.gather(
        producer_task(producer),
        consumer_task(consumer1),
        consumer_task(consumer2),
        consumer_task(consumer3),
        consumer_task(consumer4),
        consumer_task(consumer5)
    )

    print('------------------------------------- show total infor -------------------------------------')
    stream_info = await group_manager.get_stream_info(group_manager.stream_key)
    print(f'stream_key: {group_manager.stream_key} stream info : {stream_info}')

    await show_groups_infor(group1)
    await show_groups_infor(group2)


if __name__ == '__main__':
    asyncio.run(main())
