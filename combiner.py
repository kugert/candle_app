import os
import json
import asyncio
import websockets
import redis
import concurrent.futures as treading

from time import time, sleep
from unsync import unsync


class CandleRepository:
    def __init__(self, redis_params: dict = {'host': 'redis', 'port': 6379, 'db': 0}, periods: list = [1, 5, 15, 60, 300, 900], uri: str = ''):
        self.redis_conn = redis.Redis(**redis_params)
        self.periods = periods
        self.uri = uri if uri else os.environ.get('URI')
        self.codes = []

    def __call__(self):
        tasks = [
            self.__redis_manager(),
            self.__data_flush()
        ]

        [t.result() for t in tasks]

    def get(self, from_at, period) -> list:
        rv = []
        key_list = [self.__get_key(_code, from_at, period)
                    for _code in self.codes]
        for key in key_list:
            rv += self.__get_from_redis(key)
        return rv

    def clear(self, before: int):
        self.clear_before = int(time()) - before * 60

    @unsync
    async def __redis_manager(self):
        async with websockets.connect(self.uri) as ws:
            while True:
                msg = await ws.recv()
                data = json.loads(msg)

                if data.get('code') and data['code'] not in self.codes:
                    self.codes.append(data['code'])

                with treading.ThreadPoolExecutor() as executor:
                    executor.map(
                        lambda x: self.__save_to_redis(data, x),
                        self.periods
                    )

    @unsync
    def __data_flush(self):
        key_list = []
        while True:
            for _code in self.codes:
                key_list += [self.__get_key(_code, self.clear_before, x)
                             for x in self.periods]
            [self.redis_conn.delete(_k) for _k in set(key_list)]
            sleep(0.75)

    @staticmethod
    def __get_key(code, at, period):
        at_rounded = int(float(at) / period) * period
        return f'at-{code}-{period}-{at_rounded}'

    def __save_to_redis(self, val, period):
        key = self.__get_key(val['code'], val['at'], period)
        self.redis_conn.xadd(key, val, id='*')

    def __get_from_redis(self, key):
        data = self.redis_conn.xrange(key, min='-', max='+')
        return list(map(lambda x: x[1], data))

