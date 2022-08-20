import concurrent.futures
import time

start = time.perf_counter()

def foo(i):
    print(f"{i} | Thread #{i % 16}")
    time.sleep(3)
    
if __name__ == '__main__':

    with concurrent.futures.ProcessPoolExecutor() as executor:
        _ = [executor.submit(foo, i) for i in range(1, 48)]
        
        # a = concurrent.futures.as_completed(results)
        
    finish = time.perf_counter()

    print(f'Finished in {round(finish-start, 2)} second(s)')