"""
name: conftest
author：kavinbj
createdAt: 2021/7/20
version: 2.0.0
description:

pytest --u root --p password --url localhost -vs --cov --cov-report=html

"""
import asyncio
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--u", action="store", default="",
        help="redis username"
    )
    parser.addoption(
        "--p", action="store", default="",
        help="redis password"
    )
    parser.addoption(
        "--db", action="store", default=1,
        help="redis db"
    )

    parser.addoption(
        "--url", action="store", default='localhost',
        help="redis base url"
    )

    parser.addoption(
        "--port", action="store", default='6379',
        help="redis port"
    )

    parser.addoption(
        "--prefix", action="store", default='redis',
        help="redis port"
    )


@pytest.fixture(scope='session')
def get_redis_url(request):
    username = request.config.getoption("--u")
    password = request.config.getoption("--p")
    db = request.config.getoption("--db")
    base_url = request.config.getoption("--url")
    port = request.config.getoption("--port")
    prefix = request.config.getoption("--prefix")

    redis_url = f'{prefix}://{username}{":" if username and password else ""}{password}@{base_url}:{port}/{db}'
    print(f'Command line: pytest --u username --p password, get redis url is {redis_url}, please check it,  ')
    return redis_url


@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
