from types import NoneType
import time

class Benchmark:
    
    def __init__(self):
        self.repetition = None
        self.avg_exec = None

    def run_benchmark(self, con, ds) -> bool:
        sum_exec_time = 0
        
        for i in range(0, self.repetition):
            start = time.time()
            for i in range(0, len(ds)):
                con.set(ds[i].get('_id'), ds[i].get('name'))
            end = time.time()
            time_seconds = end - start 
            
            sum_exec_time += time_seconds
            con.flush() # Flush database
    
        self.avg_exec = sum_exec_time / self.repetition
        return True
    
    def get_repetition(self) -> int:
        return self.repetition
    
    def get_average_execution_time(self) -> float:
        return self.avg_exec
    
    def set_repetition(self, rep: int) -> NoneType:
        self.repetition = rep
        
    def set_average_execution_time(self, avg: float) -> NoneType:
        self.avg_exec = avg
