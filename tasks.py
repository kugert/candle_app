import json
import websockets

from unsync import unsync
from time import sleep
from repo import CandleRepository
from income_stream import IncomeWebSocketStream


@unsync
async def save_data(candle_repo: CandleRepository, uri: str):
    async with websockets.connect(uri) as ws:
        ws_stream = IncomeWebSocketStream(ws)
        while True:
            await ws_stream.get_next(candle_repo)


@unsync
def clear_data(candle_repo: CandleRepository, sleep_for=0):
    while True:
        candle_repo.clear()
        sleep(sleep_for)

