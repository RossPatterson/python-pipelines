# pipethread module

import threading
import time
import pipeLogger

class Cond:
  waiting = 0
  def __init__(self):
    self.cond = threading.Condition()
  def wait(self, thread, msg=""):
    thread.log('start wait ' + msg)
    self.waiting += 1
    self.cond.wait() 
    self.waiting -= 0
    thread.log('end wait ' + msg)

class Threads(list):
  """collection of thread objects"""
  def __init__(self):
    pass
  def add(self, target, name, *args):
    thread = Thread(target=target, name=name, args=args)
    self.append(thread)
  def startAll(self):
    for thread in self:
      thread.start()
  def start(self, index=None):
      self[index].start()
  def joinAll(self, delay):
    for thread in self:
      thread.join(delay)
  def join(self, delay):
      self[index].join(delay)
  def cleanup(self):
    for thread in threading.enumerate():
      try:
        cond = thread.interface.cond
        cond.acquire()
      except:
        continue
      cond.notify()
      cond.release()        
    
class Thread(threading.Thread):
  """Each thread is one of these, added by Threads.add"""
  logRecs = []
  cond = None
  def __init__(self, target, name, args):
    threading.Thread.__init__(self, target=target, name=name, args=args)
  def log(self, *args):
    l = (self.threadName, time.time()-self.start, args)
    pipeLogger.logger.write(str(l))
    self.logRecs.append(l)

if __name__ == "__main__":
  # tests
  # create a couple of threads which will
  #  assign something to their interface
  #  wait (timer, cond)
  # report assigned things
  # terminate the cond
  # ensure threads are stopped
  class R:
    def start(self, *args):
      print(args)
      self.sThread = threading.currentThread()
      self.sThread.args = args
      pipeLogger.logger.warning('asdf')
  threads = Threads()
  r1 = R()
  threads.add(r1.start, 'stage', 1, 2, 3)
  threads.startAll()
  threads.joinAll(.01)
  print(threads[0].args)
  #print threads
  #threads.cleanup()

  def dumpLog():
    for r in threads:
      print(r.logRecs)

  dumpLog()

