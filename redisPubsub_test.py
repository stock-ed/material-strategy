import time
from redisPubsub import RedisPubSub, RedisSubscriber


def setup_function():
    """
    Setup the test function
    """
    global data1
    data1 = {'data': 2}
    global data2
    data2 = {}


def test_redisSubscriber():
    app1 = RedisPubSub('app1')
    def x(x): return data2 = data1
    app2 = RedisSubscriber('app2', None, callback=x)
    app2.start()
    app1.publish(data1)
    time.sleep(1)
    assert data2 == data1
