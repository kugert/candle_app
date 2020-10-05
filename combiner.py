import os
import json
import asyncio
import websockets
import redis
import concurrent.futures as treading

from time import time, sleep
from unsync import unsync

URI = os.environ.get('URI')


class CandleRepository:
    def __init__(self, redis_connection: redis.Redis, periods: list = [1, 5, 15, 60, 300, 900]):
        self.__redis_conn = redis_connection
        self.__periods = periods
        self.__codes = []

    def get(self, from_at, period) -> list:
        rv = []
        key_list = [self.__get_key(_code, from_at, period)
                    for _code in self.__codes]
        for key in key_list:
            rv += self.__get_from_redis(key)
        return rv

    def clear_before(self, before: 'int (seconds)'):
        self.__clear_before = int(time()) - before * 60

    @unsync
    async def store(self):
        async with websockets.connect(URI) as ws:
            while True:
                msg = await ws.recv()
                data = json.loads(msg)

                if data.get('code') and data['code'] not in self.__codes:
                    self.__codes.append(data['code'])

                with treading.ThreadPoolExecutor() as executor:
                    executor.map(
                        lambda x: self.__save_to_redis(data, x),
                        self.__periods
                    )

    @unsync
    def clear(self):
        key_list = []
        while True:
            for _code in self.__codes:
                key_list += [self.__get_key(_code, self.__clear_before, x)
                             for x in self.__periods]
            [self.__redis_conn.delete(_k) for _k in set(key_list)]
            sleep(0.5)

    @staticmethod
    def __get_key(code, at, period):
        at_rounded = int(float(at) / period) * period
        return f'at-{code}-{period}-{at_rounded}'

    def __save_to_redis(self, val, period):
        key = self.__get_key(val['code'], val['at'], period)
        self.__redis_conn.xadd(key, val, id='*')

    def __get_from_redis(self, key):
        data = self.__redis_conn.xrange(key, min='-', max='+')
        redis_list = list(map(lambda x: x[1], data))
        return [self.__convert_byte_dict(x) for x in redis_list]

    @staticmethod
    def __convert_byte_dict(data: dict):
        _keys = list(map(lambda x: x.decode('utf-8'), data.keys()))
        _vals = list(map(lambda x: x.decode('utf-8'), data.values()))
        return dict(zip(_keys, _vals))

