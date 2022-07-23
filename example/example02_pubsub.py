"""
name: examole02_pubsub
author：kavinbj
createdAt: 2021/7/18
version: 2.0.0
description:

"""
import asyncio
import sys
import time

sys.path.append("..")
from aio_redis_mq import MQProducer, MQConsumer

_redis_url = 'redis://root:kavin321@localhost/1'


async def producer_task(producer):
    for _ in range(0, 10):
        await asyncio.sleep(1)
        send_msg_id = await producer.send_message({'msg': f'msg_{_}', 'content': time.strftime("%Y-%m-%d %H:%M:%S")})
        print(f'producer_task time at {time.strftime("%Y-%m-%d %H:%M:%S")}', f'message id={send_msg_id}')


async def consumer_task(consumer: MQConsumer, consumer_index: int):
    for _ in range(0, 10):
        msg = await consumer.block_read_messages(block=1500)
        print(f'consumer_{consumer_index} block read message', msg)


async def main():
    # one producer
    producer = MQProducer('pub_stream', redis_name='_redis_local', redis_url=_redis_url)

    # three consumer
    consumer1 = MQConsumer('pub_stream', redis_name='_redis_local', redis_url=_redis_url)
    consumer2 = MQConsumer('pub_stream', redis_name='_redis_local', redis_url=_redis_url)
    consumer3 = MQConsumer('pub_stream', redis_name='_redis_local', redis_url=_redis_url)

    await asyncio.gather(
        producer_task(producer),
        consumer_task(consumer1, 1),
        consumer_task(consumer2, 2),
        consumer_task(consumer3, 3)
    )


if __name__ == '__main__':
    asyncio.run(main())



