#!/usr/bin/env python3
# worker.py
from yadtq import create_yadtq
from yadtq.api.worker import TaskWorker
import threading
import time

# Define task handlers
def add(a, b):
    time.sleep(2)  # Simulate work
    return a + b

def multiply(a, b):
    time.sleep(2)  # Simulate work
    return a * b
def sub(a, b):
    time.sleep(2)
    return a - b
def divide(a, b):
    time.sleep(2)
    return a / b
# Create task handlers dictionary
task_handlers = {
    'add': add,
    'multiply': multiply,
    'sub': sub,
    'divide': divide,
 #   'add1': add,
  #  'multiply1': multiply,
   # 'sub1': sub,
    #'divide1': divide,
#    'add2': add,
 #   'multiply2': multiply,
  #  'sub2': sub,
   # 'divide2': divide,
    #'add3': add,
#    'multiply3': multiply,
 #   'sub3': sub,
  #  'divide3': divide,
   # 'add4': add,
    #'multiply4': multiply,
   # 'sub4': sub,
    #'divide4': divide
}

def run_worker(worker_id, broker, result_store):
    worker = TaskWorker(worker_id, task_handlers, broker, result_store)
    worker.start()

def main():
    # Create YADTQ instance
    broker, result_store = create_yadtq()

    # Start workers
    worker_threads = []
    for i in range(3):  # Start 3 workers
        thread = threading.Thread(
            target=run_worker,
            args=(f"worker_{i}", broker, result_store)
        )
        thread.daemon = True
        thread.start()
        worker_threads.append(thread)

    # Keep the main thread running to keep the workers alive
    print("Workers are running. Press Ctrl+C to exit.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down workers.")

if __name__ == "__main__":
    main()

