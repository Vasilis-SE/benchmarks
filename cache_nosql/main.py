import pandas as pd
import sys
sys.path.append('../')

import dataset
import cache_nosql.models.dragonfly as dragonflydb
import cache_nosql.models.redis as redisdb
import benchmark

if __name__ == "__main__":
    _bench = benchmark.Benchmark()
    _ds = dataset.Dataset()
    _redis = redisdb.RedisImplementationClass()
    _dragonfly = dragonflydb.DragonflyImplementationClass()
    
    try:
        # Connect to databases
        _redis.connect()
        _dragonfly.connect()
        
        # Setup dataset
        _ds.set_name("names")
        _ds.generate_path()
        _ds.fetch_ds()
        
        # ======== Benchmarking ========
        benchmark_list_results = []
        
        # Benchmark 100 registries for 10 repetitions
        _bench.reset_benchmark()
        _bench.set_database_name('redis')
        _bench.set_repetitions(10)
        _bench.run_benchmark(_redis, _ds.get_dataset()[0:100], True)
        _bench.print_benchmark()
        benchmark_list_results.append(_bench.get_benchmark_dict())
        
        _bench.reset_benchmark()
        _bench.set_database_name('dragonfly')
        _bench.set_repetitions(10)
        _bench.run_benchmark(_dragonfly, _ds.get_dataset()[0:100], True)
        _bench.print_benchmark()
        benchmark_list_results.append(_bench.get_benchmark_dict())


        # Dataframe
        df = pd.DataFrame.from_dict(benchmark_list_results)
        df.columns.values[0] = 'Database'
        df.columns.values[1] = '# Repetitions'
        df.columns.values[2] = 'Avg Execution Time (s)'

        print(df)

 
    
    except Exception as e:
        print(str(e))
        sys.exit()
        
    
    