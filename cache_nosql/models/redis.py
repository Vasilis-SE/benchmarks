import redis

from cache_nosql.interfaces.cache_functionalities import CacheFunctionalitiesInterface

class RedisBenchmarkClass(CacheFunctionalitiesInterface): 
    
    def __init__(self):
        self.connector = None
        
    def connect(self) -> bool:
        self.connector = redis.Redis(
            host='localhost',
            port=8743)
        
        if not self.connector:
            raise Exception("[Error] Could not connect with redis database...")
        return True
 
    def set(self, key: str, value) -> bool:
        return self.connector.set(key, value)
        
    def get(self, key: str) -> str:
        return self.connector.get(key)
        
    def flush(self) -> bool:
        result = self.connector.flushall()
        print('The result of the flushall is: {} '.format(result))
        return True if result is 'OK' else False
  


