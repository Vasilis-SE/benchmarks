import json
import time

import dragonfly_bench
import redis_bench

if __name__ == "__main__":
    _redis = redis_bench.RedisBenchmarkClass()
    _dragonfly = dragonfly_bench.DragonflyBenchmarkClass()
    
    try:
        times = 50
        
        _redis.connect()
        _dragonfly.connect()
        
        names_ds = open('../datasets/names.json')
        names = json.load(names_ds)

        # Redis 100 registries set
        sum_benchmarks = 0
        for j in range(0, times):
            start = time.time()
            for i in range(0,100):
                _redis.set(names[i].get('_id'), names[i].get('name'))
            end = time.time()
            time_seconds = end - start
            
            sum_benchmarks += time_seconds
            _redis.flush()

        avg_bench_exec_sec = sum_benchmarks / times
        print("The average exectution time for set function on redis is: {} s".format(str(avg_bench_exec_sec)))
        
        
        # Dragonfly 100 registries set
        sum_benchmarks = 0
        for j in range(0, times):
            start = time.time()
            for i in range(0,100):
                _dragonfly.set(names[i].get('_id'), names[i].get('name'))
            end = time.time()
            time_seconds = end - start
            
            sum_benchmarks += time_seconds
            _dragonfly.flush()

        avg_bench_exec_sec = sum_benchmarks / times
        print("The average exectution time for set function on dragonfly is: {} s".format(str(avg_bench_exec_sec)))
         
    except Exception as e:
        print("[ERROR] " + str(e))
        
    
    