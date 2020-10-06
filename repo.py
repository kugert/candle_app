import redis

from time import time
from domain import Quote


class QuoteRepository:
    def __init__(self, redis_connection: redis.Redis, periods: list = [1, 5, 15, 60, 300, 900]):
        self.__redis_conn = redis_connection
        self.periods = periods
        self.codes = []

    # Public methods

    def get(self, from_at, period) -> list:
        redis_data = []
        key_list = [self.__get_key(_code, from_at, period)
                    for _code in self.codes]
        for key in key_list:
            redis_data += self.__get_from_redis(key)

        rv = [Quote(code=x.get('code', ''), value=x, at=x.get('at', 0))
              for x in redis_data]
        return rv

    def store(self, quote: Quote, period) -> None:
        key = self.__get_key(quote.code, quote.at, period)
        self.__redis_conn.xadd(key, quote.value, id='*')

    def clear(self) -> None:
        key_list = []
        for _code in self.codes:
            key_list += [self.__get_key(_code, self.__clear_before, x)
                         for x in self.periods]
        [self.__redis_conn.delete(_k) for _k in set(key_list)]

    def clear_before(self, before: 'int (seconds)'):
        self.__clear_before = int(time()) - before * 60

    # Private methods

    @staticmethod
    def __get_key(code, at, period):
        at_rounded = int(float(at) / period) * period
        return f'at-{code}-{period}-{at_rounded}'

    def __get_from_redis(self, key):
        data = self.__redis_conn.xrange(key, min='-', max='+')
        redis_list = list(map(lambda x: x[1], data))
        return [self.__convert_byte_dict(x) for x in redis_list]

    @staticmethod
    def __convert_byte_dict(data: dict):
        _keys = list(map(lambda x: x.decode('utf-8'), data.keys()))
        _vals = list(map(lambda x: x.decode('utf-8'), data.values()))
        return dict(zip(_keys, _vals))
