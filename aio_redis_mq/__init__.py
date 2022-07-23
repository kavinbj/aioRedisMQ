"""
name: __init__
author：kavinbj
createdAt: 2021/7/1
version: 2.0.0
description:






"""



__version__ = "2.0.0"
__author__ = "Kavin K"
__credits__ = "Felix Williams"

import logging
from .client import MQClient
from .redispool import RedisPool
from .producer import MQProducer
from .consumer import MQConsumer

from .group_manger import GroupManager
from .group_consumer import GroupConsumer
from .group import Group

logging.basicConfig(level=logging.DEBUG)
logging.getLogger(__name__).addHandler(logging.NullHandler())


__all__ = [
    "MQClient",
    "RedisPool",
    "MQProducer",
    "MQConsumer",
    'GroupManager',
    'Group',
    'GroupConsumer'
]

