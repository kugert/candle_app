import os
import json
import websockets
from domain import Quote


class IncomeWebSocketStream:
    def __init__(self, ws_connect):
        self.__ws = ws_connect

    async def get_next(self) -> Quote:
        msg = await self.__ws.recv()
        data = json.loads(msg)
        rv = Quote(code=data.get('code', ''),
                   value=data,
                   at=data.get('at', 0))
        return rv


class IncomeRabbitStream:
    def __init__(self, connection):
        self.connection = connection

    def get_next(self):
        pass
