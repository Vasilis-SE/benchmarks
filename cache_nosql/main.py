import pandas as pd
import sys
import re
import numpy as np
from matplotlib import pyplot as plt 

sys.path.append('../')

import dataset
import pdf_converter as pdf
import cache_nosql.models.dragonfly as dragonflydb
import cache_nosql.models.redis as redisdb
import benchmark

def set_cmd_benchmark_suite(flush):
    clean_results()
    
    # Set command Benchmarking 
    for idx, suite in enumerate(_set_cmd_benchmark_suites):
        for con in connectors:
            _bench.reset_benchmark()
            _bench.set_database_name(re.sub(
                'ImplementationClass$', 
                '', 
                type(con.get('instance')).__name__))
            _bench.set_repetitions(5)
            _bench.set_chunk_size(suite.get('ds_size'))
            
            # Determin begining of data chunk
            chunk_beginning = 0
            if suite.get('append'):
                if idx != 0:
                    chunk_beginning = _set_cmd_benchmark_suites[idx - 1].get('ds_size')

            # Perform the set operation
            _bench.run_set_benchmark(
                con.get('instance'), 
                _ds.get_dataset()[chunk_beginning:suite.get('ds_size')], 
                flush)
            
            bench_result = _bench.get_benchmark_dict()
            con.get('results').append(bench_result.get('avg_exec'))
            _benchmark_list_results.append(bench_result)
  
def get_cmd_benchmark_suite():
    clean_results()
 
def render_benchmark_set_cmd_results():
    X_axis = np.arange(len(_set_cmd_benchmark_suites))
    X_labels = [d['ds_size'] for d in _set_cmd_benchmark_suites]
    
    for idx, con in enumerate(connectors):
        plt.bar((X_axis + (idx * 0.25)),
            con.get('results'),
            width=0.25,
            label=re.sub(
                'ImplementationClass$', 
                '',
                type(con.get('instance')).__name__), 
            color=con.get('c')) 
        
    plt.xticks(X_axis, X_labels)
    plt.title('Set Command Benchmarks') 
    plt.xlabel('Repetitions')
    plt.ylabel('Average Execution Time')
    plt.legend()
    # plt.show()
    plt.savefig('./benchmarks/set_cmd.png')

    # Dataframe
    df = pd.DataFrame.from_dict(_benchmark_list_results)
    df.columns.values[0] = 'Database'
    df.columns.values[1] = '# Repetitions'
    df.columns.values[2] = '# of Sets'
    df.columns.values[3] = 'Avg Execution Time (s)'

    _pdf.pandas_df_to_pdf(df, './benchmarks/set_cmd.pdf')

    print(df)
  
def clean_results():
    global _benchmark_list_results
    
    _benchmark_list_results = []
    for con in connectors:
        con['results'] = []       
       
if __name__ == "__main__":
    _bench = benchmark.Benchmark()
    _ds = dataset.Dataset()
    _pdf = pdf.PDFConverter()
    _redis = redisdb.RedisImplementationClass()
    _dragonfly = dragonflydb.DragonflyImplementationClass()

    _benchmark_list_results = []        
    _set_cmd_benchmark_suites = [
        {'ds_size': 100, 'append': False},
        {'ds_size': 500, 'append': False},
        # {'ds_size': 1000, 'flush': True},
        # {'ds_size': 2000, 'flush': True},
        # {'ds_size': 5000, 'flush': True}
    ]
    
    connectors = [
        {'instance': _redis, 'c': 'red', 'results': []},
        {'instance': _dragonfly, 'c': 'mediumorchid', 'results': []}
    ]

    try:
        for con in connectors:
            con.get('instance').connect()

        # Setup dataset
        _ds.set_name("names")
        _ds.generate_path()
        _ds.fetch_ds()
        
        # Set command benchmark with static data size inside store
        set_cmd_benchmark_suite(flush=True)
        render_benchmark_set_cmd_results()
        
        for con in connectors:                
            for suite in _set_cmd_benchmark_suites:
                _bench.run_set_benchmark(
                    con.get('instance'), 
                    _ds.get_dataset()[0:suite.get('ds_size')], 
                    suite.get('flush'))
        
        
        get_cmd_benchmark_suite()
 
    except Exception as e:
        print(str(e))
        sys.exit()
        
    
