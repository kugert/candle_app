import json
import websockets
import concurrent.futures as treading

from unsync import unsync
from time import sleep
from repo import QuoteRepository
from income_stream import IncomeWebSocketStream


@unsync
async def save_data(quote_repo: QuoteRepository, uri: str):
    async with websockets.connect(uri) as ws:
        ws_stream = IncomeWebSocketStream(ws)
        while True:
            quote = await ws_stream.get_next()

            if quote.code and quote.code not in quote_repo.codes:
                quote_repo.codes.append(quote.code)

            with treading.ThreadPoolExecutor() as executor:
                executor.map(
                    lambda x: quote_repo.store(quote, x),
                    quote_repo.periods
                )


@unsync
def clear_data(quote_repo: QuoteRepository, sleep_for=0):
    while True:
        quote_repo.clear()
        sleep(sleep_for)

