import os
import json
import websockets

from unsync import unsync
from repo import CandleRepository
import concurrent.futures as treading


URI = os.environ.get('URI')


class IncomeWebSocketStream:
    def __init__(self, ws_connect):
        self.ws = ws_connect

    async def get_next(self, candle_repo: CandleRepository):
        msg = await self.ws.recv()
        data = json.loads(msg)

        if data.get('code') and data['code'] not in candle_repo.codes:
            candle_repo.codes.append(data['code'])

        with treading.ThreadPoolExecutor() as executor:
            executor.map(
                lambda x: candle_repo.store(data, x),
                candle_repo.periods
            )


class IncomeRabbitStream:
    def __init__(self, connection):
        self.connection = connection

    def get_next(self):
        pass
