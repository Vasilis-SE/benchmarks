import redis

class RedisBenchmarkClass: 
    
    def __init__(self):
        self.connector = None
        
    def connect(self):
        self.connector = redis.Redis(
            host='localhost',
            port=8743)