import time
from datetime import datetime

from repo import QuoteRepository
from redis import Redis
from domain import Quote


def timestamp_amount_checker():
    date = datetime(2020, 7, 16, 12, 0, 0)
    timestamp = int(datetime.timestamp(date)) - 1
    r_conn = Redis(host='localhost', port=6379, db=0)
    _code = 'HUI/NYA'

    q_rep = QuoteRepository(r_conn)
    q_rep.codes.append(_code)
    timestamp_list = [timestamp + p for p in q_rep.periods]

    for ts in timestamp_list:
        data = Quote(code=_code, value=123, at=ts)
        for period in q_rep.periods:
            q_rep.store(data, period)

    rv_data = [q_rep.get(timestamp + 1, p) for p in q_rep.periods]

    for d_list in rv_data:
        for _d in d_list:
            print(f'rv: {_d.code} - {_d.value} - {_d.at}')
        print('---------------------')

    print(len(rv_data) == 21)


if __name__ == "__main__":
    timestamp_amount_checker()
