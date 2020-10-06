class Quote:
    def __init__(self, *args, **kwargs):
        self.redis_data = kwargs.get('redis_data', None)
        self.stream_data = kwargs.get('stream_data', None)

