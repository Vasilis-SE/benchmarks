import pandas as pd
import sys
import re
import numpy as np
from matplotlib import pyplot as plt 
from matplotlib.backends.backend_pdf import PdfPages

sys.path.append('../')

import dataset
import cache_nosql.models.dragonfly as dragonflydb
import cache_nosql.models.redis as redisdb
import benchmark


def clean_results():
    for con in connectors:
        con['results'] = []
    
def _draw_as_table(df, pagesize):
    alternating_colors = [['white'] * len(df.columns), ['lightgray'] * len(df.columns)] * len(df)
    alternating_colors = alternating_colors[:len(df)]
    fig, ax = plt.subplots(figsize=pagesize)
    ax.axis('tight')
    ax.axis('off')
    the_table = ax.table(cellText=df.values,
                        rowLabels=df.index,
                        colLabels=df.columns,
                        rowColours=['lightblue']*len(df),
                        colColours=['lightblue']*len(df.columns),
                        cellColours=alternating_colors,
                        loc='center')
    return fig     
        
def dataframe_to_pdf(df, filename, numpages=(1, 1), pagesize=(11, 8.5)):
  with PdfPages(filename) as pdf:
    nh, nv = numpages
    rows_per_page = len(df) // nh
    cols_per_page = len(df.columns) // nv
    for i in range(0, nh):
        for j in range(0, nv):
            page = df.iloc[(i*rows_per_page):min((i+1)*rows_per_page, len(df)),
                           (j*cols_per_page):min((j+1)*cols_per_page, len(df.columns))]
            fig = _draw_as_table(page, pagesize)
            if nh > 1 or nv > 1:
                # Add a part/page number at bottom-center of page
                fig.text(0.5, 0.5/pagesize[0],
                         "Part-{}x{}: Page-{}".format(i+1, j+1, i*nv + j + 1),
                         ha='center', fontsize=8)
            pdf.savefig(fig, bbox_inches='tight')
            
            plt.close()


def set_cmd_benchmark_suite():
    clean_results()
    
    set_cmd_benchmark_suites = [
        {'ds_size': 100, 'flush': True},
        {'ds_size': 500, 'flush': True},
        # {'ds_size': 1000, 'flush': True},
        # {'ds_size': 2000, 'flush': True},
        # {'ds_size': 5000, 'flush': True}
    ]
    
    # Set command Benchmarking 
    benchmark_list_results = []        
    for suite in set_cmd_benchmark_suites:
        for con in connectors:
            _bench.reset_benchmark()
            _bench.set_database_name(re.sub(
                'ImplementationClass$', 
                '', 
                type(con.get('instance')).__name__))
            _bench.set_repetitions(5)
            _bench.set_chunk_size(suite.get('ds_size'))
            
            _bench.run_set_benchmark(con.get('instance'), 
                                _ds.get_dataset()[0:suite.get('ds_size')], 
                                suite.get('flush'))
            # _bench.print_benchmark()
            
            bench_result = _bench.get_benchmark_dict()
            con.get('results').append(bench_result.get('avg_exec'))
            benchmark_list_results.append(bench_result)
    
    # Plot results & save image
    X_axis = np.arange(len(set_cmd_benchmark_suites))
    X_labels = [d['ds_size'] for d in set_cmd_benchmark_suites]
    
    for idx, con in enumerate(connectors):
        plt.bar((X_axis + (idx * 0.25)),
            con.get('results'),
            width=0.25,
            label=re.sub('ImplementationClass$', '', type(con.get('instance')).__name__), 
            color=con.get('c')) 
        
    plt.xticks(X_axis, X_labels)
    plt.title('Set Command Benchmarks') 
    plt.xlabel('Repetitions')
    plt.ylabel('Average Execution Time')
    plt.legend()
    # plt.show()
    plt.savefig('./benchmarks/set_cmd.png')

    # Dataframe
    df = pd.DataFrame.from_dict(benchmark_list_results)
    df.columns.values[0] = 'Database'
    df.columns.values[1] = '# Repetitions'
    df.columns.values[2] = 'Data Chunk Size'
    df.columns.values[3] = 'Avg Execution Time (s)'

    dataframe_to_pdf(df, './benchmarks/set_cmd.pdf')


    # pp = PdfPages("./benchmarks/set_cmd.pdf")
    # pp.savefig(plt, bbox_inches='tight')
    # pp.close()

    print(df)
 
  
if __name__ == "__main__":
    _bench = benchmark.Benchmark()
    _ds = dataset.Dataset()
    _redis = redisdb.RedisImplementationClass()
    _dragonfly = dragonflydb.DragonflyImplementationClass()
    
    try:
        # Connect to databases
        connectors = [
            {'instance': _redis, 'c': 'red', 'results': []},
            {'instance': _dragonfly, 'c': 'mediumorchid', 'results': []}
        ]
        for con in connectors:
            con.get('instance').connect()

        # Setup dataset
        _ds.set_name("names")
        _ds.generate_path()
        _ds.fetch_ds()
        
        set_cmd_benchmark_suite()
  

 
    
    except Exception as e:
        print(str(e))
        sys.exit()
        
    
    