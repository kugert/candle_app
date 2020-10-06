import json
import websockets
import concurrent.futures as treading

from unsync import unsync
from time import sleep
from repo import CandleRepository
from income_stream import IncomeWebSocketStream


@unsync
async def save_data(candle_repo: CandleRepository, uri: str):
    async with websockets.connect(uri) as ws:
        ws_stream = IncomeWebSocketStream(ws)
        while True:
            data = await ws_stream.get_next()

            if data.get('code') and data['code'] not in candle_repo.codes:
                candle_repo.codes.append(data['code'])

            with treading.ThreadPoolExecutor() as executor:
                executor.map(
                    lambda x: candle_repo.store(data, x),
                    candle_repo.periods
                )


@unsync
def clear_data(candle_repo: CandleRepository, sleep_for=0):
    while True:
        candle_repo.clear()
        sleep(sleep_for)

