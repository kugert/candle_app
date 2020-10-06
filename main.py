import time
from redis import Redis
from repo import CandleRepository
from tasks import save_data, clear_data


def main():
    redis_conn = Redis(host='redis', port=6379, db=0)
    candle_repo = CandleRepository(redis_conn)

    tasks = [
        save_data(candle_repo),
        clear_data(candle_repo, 0.5)
    ]

    [t.result() for t in tasks]


if __name__ == "__main__":
    main()

