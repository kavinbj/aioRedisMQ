"""
name: example03_web
author：felix
createdAt: 2022/7/18
version: 1.0.0
description:

"""
import time
from sanic import Sanic, response
import asyncio
import sys

sys.path.append("..")
from aio_redis_mq import RedisPool, MQProducer, MQConsumer

_redis_url = 'redis://root:kavin321@localhost/1'


app = Sanic(__name__)


async def consumer_task():
    consumer = MQConsumer('pub_stream', redis_name='_web_redis')
    while True:
        msg = await consumer.block_read_messages(block=0)
        await asyncio.sleep(0.5)
        print(f'consumer block read message', msg)


@app.listener('after_server_start')
async def notify_server_started(_app: Sanic, _loop):
    print('notify_server_started')
    # When the asynchronous web framework （sanic） starts, a new loop will be generated.
    # so redis client needs to be initialized here, and cache the redis name
    await RedisPool.init_redis('_web_redis', redis_url=_redis_url)

    # start consumer task
    await consumer_task()


@app.listener('after_server_stop')
async def notify_server_stoped(_app: Sanic, _loop):
    print('notify_server_stoped')
    await RedisPool.close_redis('_web_redis')


# test http://localhost:2000/pub in web browser
@app.route('/pub', methods=['GET'])
async def test1(request):
    # get redis client
    redis_client = RedisPool.get_redis_pool('_web_redis')
    async with redis_client as r:
        result = await r.get('key_counter')
        counter = (int(result) + 1) if result else 1
        await r.set('key_counter', counter)

    # create producer via cached redis name
    producer = MQProducer('pub_stream', redis_name='_web_redis')
    send_msg_id = await producer.send_message({'msg': f'message_{counter}', 'content': time.strftime("%Y-%m-%d %H:%M:%S")})
    print(f'producer request task time at {time.strftime("%Y-%m-%d %H:%M:%S")}', f'message id={send_msg_id}')

    return response.text(f'produce message_{counter} successful')


if __name__ == '__main__':
    print('main')
    app.run(host="127.0.0.1", port=2000)
