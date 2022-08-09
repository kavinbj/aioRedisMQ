"""
name: test_group_manager
author：kavinbj
createdAt: 2021/7/21
version: 2.0.0
description:

"""
import pytest

# import sys
# sys.path.append("..")
from aio_redis_mq import RedisPool, GroupManager, Group, exceptions


@pytest.mark.asyncio
async def test_clear_key(get_redis_url):
    redis_pool = RedisPool.get_redis_pool('_test_local1', redis_url=get_redis_url)
    results = await redis_pool.delete('_test_stream1', '_test_stream2', '_test_stream3')
    assert results >= 0


@pytest.mark.asyncio
async def test_group_manager_create(get_redis_url):

    group_manager = GroupManager('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)

    group_manager_cache = GroupManager('_test_stream1', redis_name='_test_local1')

    assert group_manager is group_manager_cache

    group_manager_cache2 = GroupManager('_test_stream1', redis_name='_test_local2', redis_url=get_redis_url)

    assert group_manager is not group_manager_cache2


@pytest.mark.asyncio
async def test_group_manager_instance(get_redis_url):

    with pytest.raises(exceptions.AioGroupError) as e:
        group_manager = GroupManager('', redis_name='_test_local1', redis_url=get_redis_url)
        assert isinstance(group_manager, GroupManager)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioGroupError
    assert exec_msg == 'stream key is None'

    with pytest.raises(exceptions.AioGroupError) as e:
        group_manager = GroupManager('_test_stream1', redis_name='', redis_url=get_redis_url)
        assert isinstance(group_manager, GroupManager)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioGroupError
    assert exec_msg == 'redis name is None'

    redis_name = '_not_exist_name'
    with pytest.raises(exceptions.AioRedisMQException) as e:
        group_manager = GroupManager('_no_exist_stream', redis_name=redis_name, redis_url=None)
        assert isinstance(group_manager, GroupManager)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioRedisMQException
    assert exec_msg == f'no {redis_name} cache instance, You must specify a redis_url'


@pytest.mark.asyncio
async def test_group_manager_create_group_mkstream(get_redis_url):
    # if not exist stream , should set mkstream True, otherwise raise exceptions
    group_manager = GroupManager('_not_exist_stream', redis_name='_test_local1', redis_url=get_redis_url)
    with pytest.raises(exceptions.AioGroupCreateError) as e:
        group = await group_manager.create_group('_group_name', msg_id='$', mkstream=False)
        assert isinstance(group, Group)
    # exec_msg = e.value.args[0]
    assert e.type == exceptions.AioGroupCreateError
    # assert exec_msg == 'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use ' \
    #                    'the MKSTREAM option to create an empty stream automatically.'

    with pytest.raises(exceptions.AioGroupError) as e:
        group = await group_manager.create_group('')
        assert isinstance(group, Group)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioGroupError
    assert exec_msg == 'group name is None'

    with pytest.raises(exceptions.AioGroupCreateError) as e:
        group = await group_manager.create_group('_error_group_name', msg_id='')
        assert isinstance(group, Group)
    exec_msg = e.value.args[0]
    assert e.type is exceptions.AioGroupCreateError
    assert exec_msg == 'Invalid stream ID specified as stream command argument'


@pytest.mark.asyncio
async def test_group_manager_create_group(get_redis_url):
    group_manager = GroupManager('_test_stream1', redis_name='_test_local1', redis_url=get_redis_url)

    group = await group_manager.create_group('_group_name1', msg_id='$', mkstream=True)
    group_cache = await group_manager.create_group('_group_name1', msg_id='$', mkstream=True)

    assert isinstance(group, Group)
    assert group is group_cache


@pytest.mark.asyncio
async def test_group_manager_get_info(get_redis_url):
    group_manager = GroupManager('_test_stream2', redis_name='_test_local1', redis_url=get_redis_url)

    group1 = await group_manager.create_group('_group_name1')
    group2 = await group_manager.create_group('_group_name2')

    groups_info = await group_manager.get_groups_info()

    assert len(groups_info) == 2
    assert groups_info[0]['name'] == group1.group_name
    assert groups_info[1]['name'] == group2.group_name


@pytest.mark.asyncio
async def test_group_manager_delete_group(get_redis_url):
    group_manager = GroupManager('_test_stream2', redis_name='_test_local1', redis_url=get_redis_url)

    group1 = await group_manager.create_group('_group_name1')
    group2 = await group_manager.create_group('_group_name2')

    assert isinstance(group1, Group)
    assert isinstance(group2, Group)

    groups_info_before_delete = await group_manager.get_groups_info()

    reuslt = await group_manager.destroy_group('_group_name2')
    assert reuslt is True

    groups_info_after_delete = await group_manager.get_groups_info()

    assert len(groups_info_before_delete) == len(groups_info_after_delete) + 1
