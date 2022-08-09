"""
name: example06_group_consumer_claim
author：kavinbj
createdAt: 2021/7/18
version: 2.0.0
description:

Here, we handle idle messages and detect pending messages before block read new messages.
it will claim idle messages to other consumer, and force ack if idle messages exceed claim times
and execute the callback method，which you can handle define, to further process these idle messages,
such as storing them in error stream .
And, it will detect pending messages before blocking read messages when the server starts.

"""
import asyncio
import time
import logging
# import sys
# sys.path.append("..")
from aio_redis_mq import MQProducer, GroupManager, Group, GroupConsumer

_redis_url = 'redis://root:xxxxx@localhost/1'

logger = logging.getLogger(__name__)


async def remove_idle_msg_callback(idle_msg):
    print('remove_idle_msg_callback', idle_msg)


async def remove_idle_message(group: Group, msg_id: str, remove_callback=None):
    """
    High Level Api
    force ack the idle message if it exceeds max_claim_times, then execute callback method
    :param group:
    :param msg_id: message id
    :param remove_callback:
    :return:
    """
    # 1、get message detail
    messages = await group.query_messages(group.stream_key, min_id=msg_id, max_id=msg_id, count=1)
    if len(messages) <= 0:
        logger.info(f'message msg_id={msg_id} not exist')
        return 0

    # 2、ack this idle_message which exceeds max_claim_times or min_idle_time
    ack_result = await group.ack_message(msg_id)

    # 3、check ack result
    if ack_result != 1:
        logger.info(f'ack message msg_id={msg_id} error')
        return 0

    # 4、callback function, for example, you can move all idle messages to a error streams by the callback
    if remove_callback is not None:
        await remove_callback(messages)

    return 1


async def get_active_consumer(group: Group):
    """
    High Level Api
    return the most active consumer with the smallest idle time
    :return:
    """
    # get consumers info in current group
    consumers_info = await group.get_consumers_info()

    # Get the consumer with the smallest idle time
    consumers_info.sort(key=lambda x: x.get('idle', 0), reverse=False)
    if len(consumers_info) <= 0:
        raise Exception('consumers count error')

    return consumers_info[0].get('name', None)


async def check_idle_messages(
    group: Group,
    max_claim_times: int = 4,
    min_claim_times: int = 2,
    min_idle_time: int = 3 * 60 * 1000,
    max_check_num: int = 30,
    current_consumer_id: str = None
):
    """
    High Level Api
    check the pending messages in the group , handle the messages if it exceeds max_claim_times or min_idle_time
    :param group:
    :param max_claim_times: force ack idle message which claim times more then this amount
    :param min_claim_times: change ownership of a pending message which claim times more then this amount
    :param min_idle_time:  filter messages that were idle less than this amount of milliseconds
    :param max_check_num: max amount of checking idle message per times
    :param current_consumer_id: name of the consumer which call this method
    :return:
    """
    pending_info = await group.get_pending_info()
    pending_num = pending_info.get('pending', 0)
    if pending_num <= 0:
        return

    print('check_idle_messages pending_info', pending_info)
    min_msg_id = pending_info.get('min', '-')
    max_msg_id = pending_info.get('max', '+')

    # Calculate minimum query count
    query_count = min(pending_num, max_check_num)
    print(min_msg_id, max_msg_id, query_count)

    detail_pending_infos = await group.query_pending_messages(min_msg_id, max_msg_id, query_count)
    # print(detail_pending_infos, len(detail_pending_infos))
    # Get the most active consumer
    active_consumer_id = await get_active_consumer(group)

    # iteration pending_infos
    for pending_msg in detail_pending_infos:
        message_id = pending_msg.get('message_id', '')
        pending_consumer_id = pending_msg.get('consumer', '')
        time_since_delivered = pending_msg.get('time_since_delivered', 0)
        times_delivered = pending_msg.get('times_delivered', 1)

        print('detail_pending_infos', pending_msg)
        # 1、exceeds max_claim_times, remove the pending message
        if times_delivered >= max_claim_times:
            await remove_idle_message(group, message_id, remove_callback=remove_idle_msg_callback)

        # 2、exceeds min_claim_times or min_idle_time, claim the pending message to other consumer
        elif times_delivered >= min_claim_times or time_since_delivered >= min_idle_time:
            print(f'time_since_delivered={time_since_delivered}', min_idle_time)
            claim_consumer_id = None

            # the pending consumer is not the current consumer, use current_consumer_id
            if current_consumer_id != pending_consumer_id:
                claim_consumer_id = current_consumer_id

            # the most active consumer is not the current consumer, use active_consumer_id
            elif active_consumer_id != pending_consumer_id:
                claim_consumer_id = active_consumer_id

            if claim_consumer_id is not None:
                claim_result = await group.claim_message(claim_consumer_id, min_idle_time, [message_id],
                                                         idle=0, justid=True)
                logger.info(f'claim idle message id={message_id} from consumer name: [{pending_consumer_id}] '
                            f'to other consumer name: [{claim_consumer_id}], result={claim_result}')


async def wait_for_messages(
    consumer: GroupConsumer,
    *stream_key: str,
    count: int = 1,
    block: int = None,
    noack: bool = False,
    check_idle_msg: bool = True,
    max_claim_times: int = 3,
    min_idle_time: int = 3 * 60 * 1000,
    max_check_num: int = 30
):
    """
    High Level Api
    blocking and read messages from streams via a consumer group.
    three steps：
      1、check and handle timeout pending message, for example, change the ownership or ack it then execute callback
      2、check whether current consumer has any pending messages, if so, return the messages
      3、block read new messages via a consumer group.
    :param consumer:
    :param stream_key: name of the stream
    :param count: if set, only return this many items, beginning with the earliest available
    :param block: number of milliseconds to wait, if nothing already present.
    :param noack: do not add messages to the PEL (Pending Entries List)
    :param check_idle_msg: whether to handle idle messages
    :param max_claim_times: force ack idle message which claim times more then this amount
    :param min_idle_time: filter messages that were idle less than this amount of milliseconds
    :param max_check_num: max amount of checking idle message per times
    :return:
    # [['mystream', [('1658029233486-0', {'msg': 'msg_6', 'content': '1658029233.485565'})]]]
    """
    # 1、check whether there is a timeout pending message in the group.
    #    If so, change the ownership of a pending message.
    #    If pending message exceeds min_idle_time idle, ack it and execute callback method
    if check_idle_msg:
        await check_idle_messages(consumer._group, max_claim_times=max_claim_times, min_idle_time=min_idle_time,
                                  max_check_num=max_check_num, current_consumer_id=consumer.consumer_id)

    # 2、check whether current consumer has any pending messages, if so, return the messages
    #    add current key to stream_key tuple
    _stream_key = tuple(set((consumer.stream_key, ) + stream_key))
    #    set the corresponding ID to 0，and can get all pending messages
    _streams = dict(zip(_stream_key, [0] * len(_stream_key)))

    pending_messages = await consumer.read_messages(_streams, count=count, noack=noack)

    if len(pending_messages) > 0 and len(pending_messages[0][1]) > 0:
        print('pending_messages', pending_messages)
        return pending_messages

    # 3、If there are no pending messages, block and read new messages
    new_messages = await consumer.block_read_messages(*_stream_key, block=block, count=count, noack=noack)
    return new_messages


async def producer_task(producer):
    await asyncio.sleep(5)
    for _ in range(0, 10):
        await asyncio.sleep(1)
        print(f'-------------------------------------{_}-------------------------------------')
        send_msg_id = await producer.send_message({'msg': f'msg_{_}', 'content': time.strftime("%Y-%m-%d %H:%M:%S")})
        print(f'group_producer send_message time at {time.strftime("%Y-%m-%d %H:%M:%S")}', f'message id={send_msg_id}')


async def consumer_task(consumer: GroupConsumer):
    for _ in range(0, 10):
        # Here is a high-level API, it will handle idle messages and detect pending messages before blocking read
        msg = await wait_for_messages(consumer, count=1, block=1500,  min_idle_time=1000)
        await asyncio.sleep(0.05)
        print(f'group_consumer {consumer.consumer_id} group={consumer.group_name} block read message', msg)
        # set task cost time 2 second
        await asyncio.sleep(2)

        # here set consumer5 error when handle task, so it can not ack the message.
        # and the message will be claimed to other consumer when it exceed claim times or idle time
        if consumer.consumer_id == 'consumer5':
            continue

        if len(msg) > 0 and len(msg[0][1]) > 0:
            msg_id = msg[0][1][0][0]
            ack_result = await consumer.ack_message(msg_id)
            print(f'group_consumer {consumer.consumer_id} group={consumer.group_name} '
                  f'ack message id={msg_id} {"successful" if ack_result else "failed"}.')


async def show_group_infor(group: Group):
    print(f'-------------------------------------{group.group_name}-------------------------------------')
    stream_info = await group.get_stream_info(group.stream_key)
    print(f'stream_key: {group.stream_key} stream info : {stream_info}')

    print('------------------------------------- groups info --------------------------------------')
    group_info = await group.get_groups_info()
    print(f'group name: {group.group_name} groups info : {group_info}')
    print('------------------------------------- consumer info -------------------------------------')
    consumer_info = await group.get_consumers_info()
    print(f'group name: {group.group_name} consumer info : {consumer_info}')
    print('------------------------------------- pending info -------------------------------------')
    pending_info = await group.get_pending_info()
    print(f'group name: {group.group_name} pending info : {pending_info}')


async def main():
    # create one producer
    producer = MQProducer('group_stream2', redis_name='_group_redis_', redis_url=_redis_url)

    # create group manager , via same stream key, same redis_name
    group_manager = GroupManager('group_stream2', redis_name='_group_redis_', redis_url=_redis_url)

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
        # consumer_task(consumer1),
        # consumer_task(consumer2),
        consumer_task(consumer3),
        consumer_task(consumer4),
        consumer_task(consumer5)
    )

    # await show_group_infor(group1)
    await show_group_infor(group2)


if __name__ == '__main__':
    asyncio.run(main())
