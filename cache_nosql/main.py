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

def set_cmd_benchmark_suite(bench_suite, flush, repetitions):
    clean_results()
    
    # Set command Benchmarking 
    for idx, suite in enumerate(bench_suite):
        for con in connectors:
            _bench.reset_benchmark()
            _bench.set_database_name(re.sub(
                'ImplementationClass$', 
                '', 
                type(con.get('instance')).__name__))
            _bench.set_repetitions(repetitions)
            _bench.set_chunk_size(suite.get('ds_size'))
            
            # Determine begining/end of data chunk
            chunk_beginning = 0
            chunk_end = suite.get('ds_size')
            if suite.get('append'):
                if idx != 0:
                    chunk_beginning = bench_suite[idx - 1].get('ds_size')
                chunk_end += chunk_beginning

            print('Beggining: {} , End: {}'.format(chunk_beginning, chunk_end))

            # Perform the set operation
            _bench.run_set_benchmark(
                con.get('instance'), 
                _ds.get_dataset()[chunk_beginning:chunk_end], 
                flush)
            
            bench_result = _bench.get_benchmark_dict()
            con.get('results').append(bench_result.get('avg_exec'))
            _benchmark_list_results.append(bench_result)
  
def get_cmd_benchmark_suite(bench_suite, repetitions):
    clean_results()
    
    for idx, suite in enumerate(bench_suite):
        for con in connectors:
            # Flush database to insert the new chunk of data.
            con.flush() 
            
            # Set benchmarking class data.
            _bench.reset_benchmark()
            _bench.set_database_name(re.sub(
                'ImplementationClass$', 
                '', 
                type(con.get('instance')).__name__))
            _bench.set_repetitions(repetitions)

    
 
def render_benchmark_set_cmd_results(bench_suite, file_name, title):
    X_axis = np.arange(len(bench_suite))
    X_labels = [d['ds_size'] for d in bench_suite]
    
    for idx, con in enumerate(connectors):
        plt.bar(
            (X_axis + (idx * 0.25)),
            con.get('results'),
            width=0.25,
            label=re.sub(
                'ImplementationClass$', 
                '',
                type(con.get('instance')).__name__), 
            color=con.get('c')) 
        
    plt.xticks(X_axis, X_labels)
    plt.title(title) 
    plt.xlabel('Data Size (# of Sets)')
    plt.ylabel('Average Execution Time')
    plt.legend()
    plt.savefig('./benchmarks/{}.png'.format(file_name))
    plt.clf()

    # Dataframe
    df = pd.DataFrame.from_dict(_benchmark_list_results)
    df.columns.values[0] = 'Database'
    df.columns.values[1] = '# Repetitions'
    df.columns.values[2] = 'Chunk Size'
    df.columns.values[3] = 'Avg Execution Time (s)'

    _pdf.pandas_df_to_pdf(df, './benchmarks/{}.pdf'.format(file_name))

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
    
    _set_cmd_append_benchmark_suites = [
        {'ds_size': 100, 'append': True},
        {'ds_size': 500, 'append': True},
        # {'ds_size': 1000, 'append': True},
        # {'ds_size': 2000, 'append': True},
        # {'ds_size': 5000, 'append': True}
    ]
    
    _get_cmd_benchmark_suites = [
        {'get': 100, 'set': 1000},
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
        
        """
        Set commannd benchmark that sets the data once without multiple repetition and fetching
        the average time performance.
        """
        set_cmd_benchmark_suite(bench_suite=_set_cmd_benchmark_suites, repetitions=1, flush=True)
        render_benchmark_set_cmd_results(
            bench_suite=_set_cmd_benchmark_suites, 
            file_name='set_cmd_single_rep',
            title='Set Benchmarks with Empty DB without Repetitions')
        
        """
        Set commannd benchmark that each iteration sets a specific amount of data to the
        databases and then flushes the entire database store to prepare for the next iteration.
        Keeping a pre defines size we can test the set command as is and without any other
        thing that can effect/hinder its performance (suc as pre existing data that slow down
        its processes).
        """
        set_cmd_benchmark_suite(bench_suite=_set_cmd_benchmark_suites, repetitions=5, flush=True)
        render_benchmark_set_cmd_results(
            bench_suite=_set_cmd_benchmark_suites, 
            file_name='set_cmd_mul_reps',
            title='Set Benchmarks with Empty DB with Repetitions')
        
        """
        Set command benchmark that each iteration appends the data to the store. 
        This is done in order to test each system how each individual command that
        sets data to the store behaves when the database contains pre-existing data 
        that possibly might hinder its performance. There is a single repetition without
        rendering the average execution time.
        """
        set_cmd_benchmark_suite(bench_suite=_set_cmd_append_benchmark_suites, repetitions=1, flush=False)
        render_benchmark_set_cmd_results(
            bench_suite=_set_cmd_append_benchmark_suites, 
            file_name='set_cmd_append_single_reps', 
            title='Set Benchmarks Appending Data to DB without Repetitions')
        
        """
        Set command benchmark that each iteration appends the data to the store. 
        This is done in order to test each system how each individual command that
        sets data to the store behaves when the database contains pre-existing data 
        that possibly might hinder its performance.
        """
        set_cmd_benchmark_suite(bench_suite=_set_cmd_append_benchmark_suites, repetitions=5, flush=False)
        render_benchmark_set_cmd_results(
            bench_suite=_set_cmd_append_benchmark_suites, 
            file_name='set_cmd_append_mul_reps', 
            title='Set Benchmarks Appending Data to DB with Repetitions')

        
        
        
        get_cmd_benchmark_suite(bench_suite=_get_cmd_benchmark_suites)
 
    except Exception as e:
        print(str(e))
        sys.exit()
        
    
