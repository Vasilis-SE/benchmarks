import dragonfly_bench
import redis_bench

if __name__ == "__main__":
    _redis = redis_bench.RedisBenchmarkClass()
    _dragon_http = dragonfly_bench.DragonflyBenchmarkClass()

    try:
        _redis.connect()
        
        _dragon_http.add_data("my-key", "my-value")
        
    except Exception as e:
        print("[ERROR] " + str(e))
        
    
    