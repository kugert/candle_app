import json
import asyncio
import websockets
import redis
import concurrent.futures as treading


URI = 'ws://localhost:18081/ws'


r = redis.Redis(host='localhost', port=6379, db=0)
period_list = [1, 5, 15, 60, 300, 900]


def save_to_redis(r_conn, val, period=1):
    key = get_key(val['code'], val['at'], period)
    data = r_conn.get(key)

    if data:
        data_dict = json.loads(data)
        data_dict['data'].append(val)
    else:
        data_dict = {'data': [val]}

    r_conn.set(key, json.dumps(data_dict))
    # print(f'{key} -> {r_conn.get(key)}\n\n')


def get_from_redis(r_conn, key):
    return r_conn.get(key)


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
                executor.map(lambda x: save_to_redis(
                    r, server_data, x), period_list)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main(URI))
