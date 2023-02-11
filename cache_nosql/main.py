import time

import dataset
import cache_nosql.models.dragonfly as dragonflydb
import cache_nosql.models.redis as redisdb
import benchmark

if __name__ == "__main__":
    _bench = benchmark.Benchmark()
    _ds = dataset.Dataset("names")
    _redis = redisdb.RedisImplementationClass()
    _dragonfly = dragonflydb.DragonflyImplementationClass()
    
    try:
        _redis.connect()
        _dragonfly.connect()
        
        
        _bench.set_repetition(10)
        _bench.run_benchmark(_redis, )
        

        # Redis 10 registries set
 
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
        
    
    