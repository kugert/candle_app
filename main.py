import os
import time
from redis import Redis
from repo import QuoteRepository
from tasks import save_data, clear_data

URI = os.environ.get('URI')


def main():
    redis_conn = Redis(host='redis', port=6379, db=0)
    quote_repo = QuoteRepository(redis_conn)

    tasks = [
        save_data(quote_repo, URI),
        clear_data(quote_repo, 0.5)
    ]

    [t.result() for t in tasks]


if __name__ == "__main__":
    main()

