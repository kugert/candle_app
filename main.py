import time
from redis import Redis
from combiner import CandleRepository
from unsync import unsync


def main(r_conn):
    candle = CandleRepository(r_conn)
    tasks = [
        candle.store(),
        candle.clear()
    ]

    [t.result() for t in tasks]


if __name__ == "__main__":
    redis_conn = Redis(host='redis', port=6379, db=0)
    main(redis_conn)

