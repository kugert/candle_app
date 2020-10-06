import json
import websockets

from unsync import unsync
from time import sleep
from repo import CandleRepository
from ws import ws_data, URI


@unsync
async def save_data(candle: CandleRepository):
    async with websockets.connect(URI) as ws:
        while True:
            await ws_data(candle, ws)


@unsync
def clear_data(candle: CandleRepository, sleep_for=0):
    while True:
        candle.clear()
        sleep(sleep_for)

