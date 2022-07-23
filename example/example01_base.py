"""
name: example01_base
author：kavinbj
createdAt: 2021/7/18
version: 2.0.0
description:

"""

import asyncio
import sys

sys.path.append("..")
from aio_redis_mq import RedisPool, MQClient, MQProducer, MQConsumer

_redis_url = 'redis://root:kavin321@localhost/1'
_username = 'root'
_password = 'kavin321'
_db = 1


#  1、You can get aioredis instances by RedisPool.get_redis_pool
async def test_redis_pool():
    # same redis_name, same instance. RedisPool cache the instance via redis_name.
    redis_pool1 = RedisPool.get_redis_pool('_local1', redis_url=_redis_url)
    redis_pool2 = RedisPool.get_redis_pool('_local1')
    assert redis_pool1 is redis_pool2

    # basic operation
    await redis_pool1.set('key1', 'value1')
    result1 = await redis_pool1.get('key1')
    assert result1 == 'value1'
    await redis_pool1.close()

    # via keyword-arguments   Response Decoding, By default decode_responses=True
    redis_pool3 = RedisPool.get_redis_pool('_local2', redis_url='redis://localhost',
                                           username=_username,
                                           password=_password,
                                           db=_db)
    # with operation
    async with redis_pool3 as r:
        result2 = await r.get('key1')
        assert result1 == result2


# 2、producer can send message
async def test_producer_message():
    # create Producer via stream key , redis_name , redis_url(can be omitted if the redis name previously cached)
    producer1 = MQProducer('mystream1', redis_name='_local2', redis_url=_redis_url)

    # create Producer via redis_pool
    redis_pool1 = RedisPool.get_redis_pool('_local2')
    producer2 = MQProducer('mystream2', redis_pool=redis_pool1)
    assert producer1.redis_pool is producer2.redis_pool
    assert producer1 is not producer2

    # produce message
    send_msg_id = await producer1.send_message({'msgkey1': 'mystream1_msgvalue1', 'msgkey2': 'mystream1_msgvalue2'})
    # query message via msg_id
    msg_content = await producer1.query_messages(producer1.stream_key, min_id=send_msg_id, max_id=send_msg_id, count=1)
    print(msg_content)
    assert msg_content[0][0] == send_msg_id
    assert msg_content[0][1] == {'msgkey1': 'mystream1_msgvalue1', 'msgkey2': 'mystream1_msgvalue2'}

    # another producer send message in another stream (stream key = mystream2)
    send_msg_id = await producer2.send_message({'msgkey1': 'mystream2_msgvalue1', 'msgkey2': 'mystream2_msgvalue2'})
    print(send_msg_id)


# 3、consumer can read message
async def test_consumer_message():
    # create consumer via stream key , redis_name
    consumer1 = MQConsumer('mystream1', redis_name='_local2', redis_url=_redis_url)
    # read messages from msg_id(0 or other id)  in single stream (mystream1)
    read_single_msgs = await consumer1.read_messages({'mystream1': 0}, count=10)
    print(read_single_msgs)

    # read messages from msg_id(0 or other id) in multi stream (mystream1 , mystream2)
    read_multi_msgs = await consumer1.read_messages({'mystream1': 0, 'mystream2': 0}, count=10)
    print(read_multi_msgs)


# 4、client can query stream info and message info , even delete message if you really want
async def test_client_query():
    # create client via redis_name , redis_url
    client = MQClient(redis_name='_local2', redis_url=_redis_url)
    stream_info = await client.get_stream_info('mystream1')
    print(stream_info)
    all_stream_msg = await client.query_messages('mystream1', min_id='-', max_id='+', count=10)
    print(all_stream_msg)


async def main():
    # aioredis instances
    await test_redis_pool()
    # producer operation
    await test_producer_message()
    # consumer operation
    await test_consumer_message()
    # Message visualization
    await test_client_query()


if __name__ == '__main__':
    asyncio.run(main())






