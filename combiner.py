import os
import json
import asyncio
import websockets
import redis
import concurrent.futures as treading

from time import time, sleep
from unsync import unsync


URI = os.environ.get('URI')
r_conn = redis.Redis(host='redis', port=6379, db=0)
PERIODS = [1, 5, 15, 60, 300, 900]
CODES = []


def main():
    tasks = [
        redis_manager(),
        data_flush()
    ]

    [t.result() for t in tasks]


@unsync
async def redis_manager():
    async with websockets.connect(URI) as ws:
        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            if data.get('code') and data['code'] not in CODES:
                CODES.append(data['code'])

            with treading.ThreadPoolExecutor() as executor:
                executor.map(
                    lambda x: save_to_redis(data, x),
                    PERIODS
                )


@unsync
def data_flush():
    key_list = []
    max_age = int(time()) - 16*60
    while True:
        for _code in CODES:
            key_list += [get_key(_code, max_age, x) for x in PERIODS]
        [r_conn.delete(_k) for _k in set(key_list)]


def get_key(code, at, period):
    at_rounded = int(float(at) / period) * period
    return f'at-{code}-{period}-{at_rounded}'


def save_to_redis(val, period):
    key = get_key(val['code'], val['at'], period)
    r_conn.xadd(key, val, id='*')


def get_from_redis(key):
    return r_conn.xrange(key, min='-', max='+')


if __name__ == "__main__":
    main()

