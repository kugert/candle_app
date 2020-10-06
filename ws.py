import os
import json
import websockets

from unsync import unsync
from repo import CandleRepository
import concurrent.futures as treading


URI = os.environ.get('URI')


async def ws_data(candle: CandleRepository, ws):
    msg = await ws.recv()
    data = json.loads(msg)

    if data.get('code') and data['code'] not in candle.codes:
        candle.codes.append(data['code'])

    with treading.ThreadPoolExecutor() as executor:
        executor.map(
            lambda x: candle.store(data, x),
            candle.periods
        )
