import time

class Benchmark:
    
    def __init__(self):
        self.reset_benchmark()
        
    def reset_benchmark(self):
        self.database = None
        self.repetitions = None
        self.size = None
        self.avg_exec = None

    def get_benchmark_dict(self) -> dict:
        return {
            'database': self.database,
            'repetitions': self.repetitions,
            'size': self.size,
            'avg_exec': self.avg_exec,
        }

    def print_benchmark(self):
        print('Database: {}'.format(self.database))
        print('Repetitions: {}'.format(self.repetitions))
        print('Size: {}'.format(self.size))
        print('Avg Execution Time: {} s'.format(self.avg_exec))
        print('------')
   
    def run_set_benchmark(self, con, ds, flush) -> bool:
        sum_exec_time = 0
        
        for i in range(0, self.repetitions):
            start = time.time()
            for i in range(0, len(ds)):
                con.set(ds[i].get('_id'), ds[i].get('name'))
            end = time.time()
            time_seconds = end - start 
            
            sum_exec_time += time_seconds
            if flush:
                con.flush() # Flush database
    
        self.avg_exec = sum_exec_time / self.repetitions
        return True
    
    # Getters / Setters
    def get_database_name(self) -> str:
        return self.database

    def get_repetitions(self) -> int:
        return self.repetitions
    
    def get_chunk_size(self) -> int:
        return self.size

    def get_average_execution_time(self) -> float:
        return self.avg_exec
    
    def set_database_name(self, db):
        self.database = db
    
    def set_repetitions(self, rep: int):
        self.repetitions = rep
        
    def set_chunk_size(self, s):
        self.size = s

    def set_average_execution_time(self, avg: float):
        self.avg_exec = avg
