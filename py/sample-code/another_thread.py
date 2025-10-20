# ThreadRunner

from threading import Thread
from time import sleep

class ThreadRunner:

    def __init__(self, task, *args):
        print("Thread runner initialize")
        self._task = task
        self._args = args
        self.thread = Thread(target=self._task, args=(self, *args))
        self.done = False
        
    def start(self):
        print("Starting Thread")
        self.thread.start()
    
    def join(self):
        self.thread.join()
    
    def _finish(self):
        self.done = True

def Task(runner, *args):
    print(f"Sleeping for {args[0]}s")
    sleep(int(args[0]))
    runner._finish()

if __name__ == "__main__":
    tr = ThreadRunner(Task, 5)
    tr.start()
    while True:
        if tr.done:
            print("tr Done")
            tr.join()
            break
        else:
            sleep(1)
            print("tr still running")
    print("All done")