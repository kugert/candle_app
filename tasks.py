import json
import websockets

from unsync import unsync
from time import sleep
from repo import CandleRepository
from income_stream import URI, IncomeWebSocketStream


@unsync
async def save_data(candle: CandleRepository):
    async with websockets.connect(URI) as ws:
        ws_stream = IncomeWebSocketStream(ws)
        while True:
            await ws_stream.get_next(candle)


@unsync
def clear_data(candle: CandleRepository, sleep_for=0):
    while True:
        candle.clear()
        sleep(sleep_for)

