import redis

class DragonflyBenchmarkClass: 
    
    def __init__(self):
        self.connector = None
        
    def connect(self):
        self.connector = redis.Redis(
            host='localhost',
            port=6379)
        
    def set(self, key, value):
        self.connector.set(key, value)
        
    def get(self, key):
        self.connector.get(key)
        
    def flush(self):
        self.connector.flushall()
