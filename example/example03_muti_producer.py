"""
name: examole01_pubsub
author：felix
createdAt: 2022/7/18
version: 1.0.0
description:

"""
import asyncio
import sys
import time

sys.path.append("..")
from aio_redis_mq import MQProducer, MQConsumer

_redis_url = 'redis://root:kavin321@localhost/1'


async def producer_task(producer: MQProducer, producer_index: int):
    for _ in range(0, 10):
        await asyncio.sleep(0.9 + producer_index * 0.1)
        send_msg_id = await producer.send_message({'msg': f'msg_1_{_}', 'content': time.strftime("%Y-%m-%d %H:%M:%S")})
        print(f'producer_{producer_index} time at {time.strftime("%Y-%m-%d %H:%M:%S")}', f'message id={send_msg_id}')


async def consumer_task(consumer: MQConsumer, consumer_index: int):
    for _ in range(0, 20):
    # while True:
        msg = await consumer.block_read_messages('pub_stream1', 'pub_stream2', block=1500)
        await asyncio.sleep(0.05)
        print(f'consumer_{consumer_index} block read message', msg)


async def main():
    # first producer
    producer1 = MQProducer('pub_stream1', redis_name='_redis_local', redis_url=_redis_url)
    # second producer
    producer2 = MQProducer('pub_stream2', redis_name='_redis_local', redis_url=_redis_url)

    # three consumer
    consumer1 = MQConsumer('pub_stream1', redis_name='_redis_local', redis_url=_redis_url)
    consumer2 = MQConsumer('pub_stream1', redis_name='_redis_local', redis_url=_redis_url)
    consumer3 = MQConsumer('pub_stream1', redis_name='_redis_local', redis_url=_redis_url)

    await asyncio.gather(
        producer_task(producer1, 1),
        producer_task(producer2, 2),
        consumer_task(consumer1, 1),
        consumer_task(consumer2, 2),
        consumer_task(consumer3, 3)
    )


if __name__ == '__main__':
    asyncio.run(main())



