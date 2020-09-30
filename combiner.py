import os
import json
import asyncio
import websockets
import redis
import concurrent.futures as treading

# default_uri = 'ws://localhost:18081/ws'
URI = os.environ.get('URI')

r_conn = redis.Redis(host='redis', port=6379, db=0)
period_list = [1, 5, 15, 60, 300, 900]


def save_to_redis(val, period=1):
    key = get_key(val['code'], val['at'], period)
    r_conn.xadd(key, val, id='*')
    # TODO: need to set expire once when key is created
    r_conn.expire(key, 16*60)


def get_from_redis(key):
    # rv: list of tuples.
    # Sample: [(b'1601465569624-0', {b'code': b'GBP/JPY', b'bid': b'43.77702805758552', b'ask': b'44.00702805758552', b'at': b'1601465569'})]
    return r_conn.xrange(key, min='-', max='+')


def get_key(code, at, period):
    at_rounded = int(float(at) / period) * period
    return f'at-{code}-{period}-{at_rounded}'


# Sample: {'code': 'GBP/JPY', 'bid': 0.5584099998744797, 'ask': 0.7884099998744797, 'at': 1601384165}
async def main(uri):
    async with websockets.connect(uri) as ws:
        while True:
            msg = await ws.recv()
            server_data = json.loads(msg)
            # print(f'{server_data}')
            with treading.ThreadPoolExecutor() as executor:
                executor.map(
                    lambda x: save_to_redis(server_data, x),
                    period_list
                )


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main(URI))
