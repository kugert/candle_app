import os
import json
import websockets


class IncomeWebSocketStream:
    def __init__(self, ws_connect):
        self.ws = ws_connect

    async def get_next(self):
        msg = await self.ws.recv()
        data = json.loads(msg)
        return data


class IncomeRabbitStream:
    def __init__(self, connection):
        self.connection = connection

    def get_next(self):
        pass
