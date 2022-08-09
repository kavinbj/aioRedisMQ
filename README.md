# aio_redis_mq
Lightweight Message Queue & Broker base on async python redis streams

# Suitable Application Environment
Modern software applications have moved from being a single monolithic unit to loosely coupled collections of services.
While this new architecture brings many benefits, those services still need to interact with each other,
 creating the need for robust and efficient messaging solutions.
 
The following problems are suitable for using message queuing：

- Asynchronous processing
- Flow control
- Service decoupling
- Connect flow computing
- As a publish / subscribe system

# Installation
```bash
pip install aio-redis-mq
```

## Quick Start ##
```python
import asyncio
import time
from aio_redis_mq import MQProducer, MQConsumer

_redis_url = 'redis://root:xxxxx@localhost/1'


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
```

## Group Consumer ##
```python
import asyncio
import time
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
```

## More Example ##
For more examples, please query the example folder.


# About Redis streams
The Redis Stream is a new data type introduced with Redis 5.0, which models a log data structure in a more abstract way. 
Redis Streams doubles as a communication channel for building streaming architectures and as a log-like data structure 
for persisting data, making Streams the perfect solution for event sourcing.

The stream type published in redis5.0 is also used to implement typical message queues. 
The emergence of this stream type meets almost all the requirements of message queues, 
including but not limited to:
- Serialization generation of message ID
- Message traversal
- Blocking and non blocking reading of messages
- Group consumption of messages
- Unfinished message processing
- Message queue monitoring

# Comparison of basic concepts
Common distributed message system, including RabbitMQ 、 RocketMQ 、 Kafka 、Pulsar 、Redis streams

Redis streams vs Kafka

|Kafka | Redis Streams | Description  |
|-----------|-------|--------|
|Record | Message| Objects to be processed in the message engine |
|Producer |Producer| Clients that publish new messages to topics |
|Consumer |Consumer| Clients that subscribe to new messages from topics |
|Consumer Group |Consumer Group| A group composed of multiple consumer instances can consume the same topic at the same time to achieve high throughput.|
|Broker |Cluster Node| servers form the storage layer. Leader-Follower replica|
|Topic | Stream Data type | Topics are logical containers that carry messages |
|partitions |Different Redis keys| Redis Streams  [Differences with Kafka (TM) partitions](https://redis.io/docs/manual/data-types/streams/#differences-with-kafka-tm-partitions)   |


# Performance
You can use the following tools for performance testing.

[OpenMessaging Benchmark Framework](https://github.com/openmessaging/benchmark)

# API Reference

### MQClient ###
client for message system, can manage and query messages. 

* #### `__init__(redis_name: Optional[str] = None, redis_url: Optional[str] = None, redis_pool: aioredis.client.Redis = None, **kwargs)` ####
    create MQ Client instance
    * `redis_name`: name for cache redis client
    * `redis_url`:  redis server url
    * `redis_pool`: aioredis.client.Redis instance, defaults to None
    
* #### `get_stream_length(stream_key: KeyT)` ####
    Returns the number of elements in a given stream.
    * `stream_key`: key of the stream.
    
* #### `query_messages(stream_key: KeyT, min_id: StreamIdT = "-", max_id: StreamIdT = "+", count: Optional[int] = None)` ####
    query message value from min_id to max_id with count limit in a given stream.
    * `stream_key`: key of the stream.
    * `min_id`: first stream ID. defaults to '-', meaning the earliest available.
    * `max_id`: last stream ID. defaults to '+', meaning the latest available.
    * `count`: if set, only return this many items, beginning with the earliest available.
    
* #### `reverse_query_messages(stream_key: KeyT, min_id: StreamIdT = "-", max_id: StreamIdT = "+", count: Optional[int] = None)` ####    
    query message value in reverse order from min_id to max_id with count limit in a given stream.
    
* #### `get_stream_info(stream_key: KeyT)` ####    
    Returns general information about the stream.
    
* #### `delete_message(stream_key: KeyT,  *ids: StreamIdT)` ####    
    Deletes one or more messages from a stream.
    * `stream_key`: key of the stream.
    * `*ids`: message ids to delete.

* #### `trim_stream(stream_key: KeyT,  maxlen: int, approximate: bool = True)` ####    
    Deletes one or more messages from a stream.    
    * `stream_key`: key of the stream.
    * `maxlen`: truncate old stream messages beyond this size
    * `maxlen`: actual stream length may be slightly more than maxlen
    
    ```python
    client = MQClient(redis_name='my_redis', redis_url='redis://root:xxxxx@localhost/0')
    
    # get stream length
    stream_length = await client.get_stream_length('_test_stream1')

    # get stream info
    stream_info = await client.get_stream_info('_test_stream1')

    assert stream_info.get('length') == stream_length

    # get first_message_info
    first_message_info = await client.query_messages('_test_stream1', count=1)
    # get last_message_info
    last_message_info = await client.reverse_query_messages('_test_stream1', count=1)

    assert first_message_info[0] == stream_info.get('first-entry')
    assert last_message_info[0] == stream_info.get('last-entry')
    ```  
    

### MQProducer <- MQClient ###
message producer, MQClient with a specific stream key

* #### `__init__(stream_key: KeyT, redis_name: str = None, redis_pool: aioredis.client.Redis = None, **kwargs)` ####
    message producer in message system based on a specific stream key.
    * `stream_key`: key of stream
    * `redis_name`: name for cache redis client
    * `redis_url`:  redis server url
    * `redis_pool`: aioredis.client.Redis instance, defaults to None

* #### `send_message(message: Dict[FieldT, EncodableT], msg_id: StreamIdT = "*", maxlen: int = None, approximate: bool = True)` ####
    __Coroutine__. send message content to a stream which is a message container, and return message id.
    * `message`:  dict of field/value pairs to insert into the stream
    * `msg_id`:  Location to insert this record. By default it is appended.
    * `maxlen`:  max number of messages, truncate old stream members beyond this size
    * `approximate`:  actual stream length may be slightly more than maxlen
    ```python
    producer = MQProducer('pub_stream', redis_name='my_redis', redis_url='redis://root:xxxxx@localhost/0')
    send_msg_id = await producer.send_message({'msg_key1': 'value1', 'msg_key2': 'value2'})
    ```  
    
### MQConsumer <- MQClient ###
message consumer, MQClient with a specific stream key  
 
* #### `__init__(stream_key: KeyT, redis_name: str = None, redis_pool: aioredis.client.Redis = None, **kwargs)` #### 
    message consumer in message system based on a specific stream key.
    * `stream_key`: key of stream
    * `redis_name`: name for cache redis client
    * `redis_url`:  redis server url
    * `redis_pool`: aioredis.client.Redis instance, defaults to None

* #### `read_messages(streams: Dict[KeyT, StreamIdT], count: Optional[int] = None)` ####
    __Coroutine__. read messages from streams as message containers
    * `streams`:  a dict of stream keys to stream IDs, where IDs indicate the last ID already seen.
    * `count`:  if set, only return this many items, beginning with the earliest available.
    
* #### `block_read_messages(*stream_key: KeyT, count: Optional[int] = None, block: Optional[int] = None,)` ####
    __Coroutine__. Block and monitor multiple streams for new data.
    * `stream_key`:  key of the stream.
    * `count`:  if set, only return this many items, beginning with the earliest available. 
    * `block`:  number of milliseconds to wait, if nothing already present.

    ```python
    consumer = MQConsumer('pub_stream', redis_name='my_redis', redis_url='redis://root:xxxxx@localhost/0')
  
    # block read new message
    new_msg = await consumer.block_read_messages(block=1500)
    
    # read messages from msg_id(0 or other id)  in single stream (pub_stream)
    read_msgs = await consumer.read_messages({'pub_stream': 0}, count=10)
    ```  

### GroupManager ###
* #### `__init__(stream_key: KeyT, redis_name: str = None, **kwargs)` #### 
    group manager in message system based on a specific stream key.
    * `stream_key`: key of stream
    * `redis_name`: name for cache redis client
    * `redis_url`:  redis server url
    
* #### `create_group(group_name: GroupT, msg_id: StreamIdT = "$", mkstream: bool = True)` #### 
    Create a new group consumer associated with a stream
    * `group_name`: name of the consumer group
    * `msg_id`: ID of the last item in the stream to consider already delivered.
    * `mkstream`:  a boolean indicating whether to create new stream

* #### `destroy_group(group_name: GroupT)` #### 
    Destroy a consumer group
    * `group_name`: name of the consumer group
    
* #### `get_groups_info()` #### 
    Returns general information about the consumer groups of the stream.

    ```python
    group_manager = GroupManager('pub_stream', redis_name='my_redis', redis_url='redis://root:xxxxx@localhost/0')
  
    # create group
    group = await group_manager.create_group('group')
    ```      

### Group ###
* #### `create_consumer(consumer_id: ConsumerT)` #### 
    create a consumer instance in group
    * `consumer_id`: id of consumer.

* #### `delete_consumer(consumer_id: ConsumerT)` #### 
    Remove a specific consumer from a consumer group.
    * `consumer_id`: id of consumer.
    
* #### `set_msg_id(msg_id: StreamIdT)` #### 
    Set the consumer group last delivered ID to something else.
    * `msg_id`: ID of the last item in the stream to consider already delivered

* #### `get_groups_info()` #### 
    Returns general information about the consumer groups of the stream.

* #### `get_consumers_info()` #### 
    Returns general information about the consumers in the group. only return consumer which has read message

* #### `get_pending_info()` #### 
    Returns information about pending messages of a group.

* #### `query_pending_messages(min_msg_id: Optional[StreamIdT], max_msg_id: Optional[StreamIdT], count: Optional[int], consumer_id: Optional[ConsumerT] = None)` #### 
    Returns information about pending messages, in a range.
    * `min_msg_id`: minimum message ID
    * `max_msg_id`: maximum message ID
    * `count`: number of messages to return
    * `consumer_id`: id of a consumer to filter by (optional)
    
* #### `ack_message(*msg_id: StreamIdT)` #### 
    Acknowledges the successful processing of one or more messages.
    * `msg_id`: message ids to acknowledge.

* #### `claim_message(consumer_id: ConsumerT, min_idle_time: int, msg_ids: Union[List[StreamIdT], Tuple[StreamIdT]], idle: Optional[int] = None, time: Optional[int] = None, retrycount: Optional[int] = None, force: bool = False, justid: bool = False)` #### 
    Changes the ownership of a pending message. In the context of a stream consumer group, 
    this command changes the ownership of a pending message, 
    so that the new owner is the consumer specified as the command argument.
    * `consumer_id`: name of a consumer that claims the message.
    * `min_idle_time`: filter messages that were idle less than this amount of milliseconds
    * `msg_ids`: non-empty list or tuple of message IDs to claim
    * `idle`: Set the idle time (last time it was delivered) of the message in ms
    * `time`: optional integer. This is the same as idle but instead of a relative amount of milliseconds,
                     it sets the idle time to a specific Unix time (in milliseconds).
    * `retrycount`: optional integer. set the retry counter to the specified value.
                           This counter is incremented every time a message is delivered again.
    * `force`: optional boolean, false by default. Creates the pending message entry in the PEL
                      even if certain specified IDs are not already in the PEL assigned to a different client.
    * `justid`: optional boolean, false by default. Return just an array of IDs of messages successfully
                      claimed, without returning the actual message
    
    
### GroupConsumer ###

* #### `read_messages(streams: Dict[KeyT, StreamIdT], count: Optional[int] = None, noack: bool = False)` #### 
    Read from a stream via a consumer group.
    * `streams`: a dict of stream names to stream IDs, where IDs indicate the last ID already seen.
    * `count`: if set, only return this many items, beginning with the earliest available   
    * `noack`: do not add messages to the PEL (Pending Entries List) 
    
* #### `block_read_messages(*stream_key: KeyT, block: Optional[int] = None, count: Optional[int] = None, noack: bool = False)` #### 
    Block read from a stream via a consumer group.
    * `stream_key`: a list of stream key
    * `block`: number of milliseconds to wait, if nothing already present.
    * `count`: if set, only return this many items, beginning with the earliest available
    * `noack`: do not add messages to the PEL (Pending Entries List) 
    
* #### `query_pending_messages(min_msg_id: Optional[StreamIdT], max_msg_id: Optional[StreamIdT], count: Optional[int])` #### 
    Returns information about pending messages, in a range.
    * `min_msg_id`: minimum message ID
    * `max_msg_id`: maximum message ID
    * `count`: number of messages to return
    
* #### `ack_message(*msg_id: StreamIdT)` #### 
    Acknowledges the successful processing of one or more messages.
    * `msg_id`: message ids to acknowledge.        
       
            
# Developer
kavinbj