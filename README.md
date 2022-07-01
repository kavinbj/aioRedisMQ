# aioRedisMQ
Lightweight Message Queue & Broker base on async python redis streams

# Suitable Application Environment
Modern software applications have moved from being a single monolithic unit to loosely coupled collections of services.
While this new architecture brings many benefits, those services still need to interact with each other,
 creating the need for robust and efficient messaging solutions.
 
The following problems are suitable for using message queuing：

- Asynchronous processing
- Flow control
- mq.png
- Connect flow computing
- As a publish / subscribe system

# About Redis streams
The Redis Stream is a new data type introduced with Redis 5.0, which models a log data structure in a more abstract way. 
Redis Streams doubles as a communication channel for building streaming architectures and as a log-like data structure 
for persisting data, making Streams the perfect solution for event sourcing.

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


# Developer
kavinbj