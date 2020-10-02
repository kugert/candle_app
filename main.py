import time
from combiner import CandleRepository


if __name__ == "__main__":
    candle = CandleRepository()
    candle()
    time.sleep(5)
    t = int(time.time()) - 2
    print(candle.get(t, 1))

