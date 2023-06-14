from  __future__ import with_statement
import threading
import time

try:    reload(stage2)
except: import stage2

try:    reload(pipethread)
except: import pipethread

try:    reload(pipeLogger)
except: import pipeLogger

class RexxStage(stage2.Stage):
  # NO DOCSTRING! as that is seen as REXX Code!
  currentStream = 0 # can be changed by SELECT
  """The scanner will determine that a stage is a REXX program.
     Such stages will be an instance of RexxStage.
  """

  # subcommands
  def readto(self, rest):    self.read(rest, "read")
  def peekto(self, rest):    self.read(rest, "peek")
  def output(self, rest):    print 'output', self.rexxVars[rest]
  def select(self, rest):    print 'select', rest
  def short(self, rest):     print 'short'
  def callpipe(self, rest):  print 'callpipe', rest
  def addpipe(self, rest):   print 'addpipe', rest
  def addstream(self, rest): print 'addstream'
  # FO BEGOUTPUT COMMIT EOFREPORT NOCOMMIT REXX SETRC SUSPEND GETRANGE SCANRANGE  
  # SCANSTRING MESSAGE ISSUEMSG MAXSTREAM STAGENUM STREAMNUM STREAMSTATE 
#  def x(self, rest): print ''
  # buld a dictionary of command : function entries
  cmds = dict((key.lower(), value) for key, value in locals().items()) 
  def fail(self, cmd): print 'unknown command', cmd
  def __init__(self, rexxString):
    self.rexxVars = {}
    self.inStreams = [RexxInStream()]
    if self.__doc__:
      self.rexxString = self.__doc__ 
    ## FO read REXX code from file
    else:
      self.rexxString = rexxString
    ## FO convert rexxString into an executable thing
  def start(self): ## FO run the executable thing
    pipeLogger.logger.info('start')
    for line in self.rexxString.split('\n'):
      if line:
        line = line.split(None, 1)
        if len(line) > 1:
          cmd, rest = line[0], line[1]
        else:
          cmd, rest = line[0], ''
        fn = self.cmds.get(cmd, None)
        if fn:
          fn(self, rest) # readto, peekto, output 
        else:
          self.fail(cmd)
  def read(self, rest, kind):
    """Implements readto and peekto.
       rest must be an expression resolving to a variable name.
       For now just expect a string literal."""
    print "Read", rest, kind
    rest = rest.strip()
    record = self.inStreams[self.currentStream].read(kind)
    if rest:
      s = rest[0]
      if s != rest[-1]:
        raise Exception, "bad string"
      if s == '"""' or s == "'''":
        if rest[:3] != rest[-3:]:
          raise Exception, "bad string"
        else:
          varName = rest[3:-3]
      else:
        varName = rest[1:-1]
      self.rexxVars[varName] = record
  """Subclass of Stage for stage that needs to be run in its own thread.
     Required for stages that must suspend (e.g. contain callpipe, readto, ...)"""
  def exit(self, rc=0):
    raise PipeException, ('exit', rc)
  def error(self, txt):
    raise PipeException, ('error', txt)
  def masterStart(self):
    t = threading(target=self.start, args=(ThreadManager(),))

    ## where do we save this thread? PipeLet need special attention!    
    self.pipeLineSet.addThread(t)
    try:
      t.start()
    except PipeException, arg:
      ## reasons:
      ##  exit(rc)
      ##
      ##
      
      if arg is None:
        pass
      elif arg[0] == 'exit':
        rc = arg[1]
      elif arg[0] == 'error':
        errorText = arg[1]
    
class RexxInStream:
  """If peek (or read) is first it must wait for run.
     Run delivers the record, clears the wait, and if peek was waiting, waits.
     The cleared read/peek returns the record.

     If run is first it must wait for peek or read.
     Peek will return the record.
     Read will clear the wait and return the record.
  """
  def __init__(self):
    self.cond = pipethread.Cond()
 
  def read(self, kind):
    """stage issued a read/peek"""
    if kind in ("read", "peek"):
      self.consume = kind == "read"
    else:
      raise Exception, 'Unexpected kind %s' % kind
    thread = threading.currentThread()
    thread.cond = self.cond

    with self.cond.cond:
      if self.cond.waiting: # run waiting
        if kind == "read":  
          thread.log("read releases run")
          self.cond.cond.notify() # release the run
        else:
          thread.log("peek lets run wait")
      else:
        self.cond.wait(thread, kind + " for run") 
    return self.record
  
  def run(self, record):  
    """record coming in from upstream"""
    thread = threading.currentThread()
    thread.cond = self.cond
    thread.log("run delivers  " + record)
    self.record = record # save so read/peek can get it

    with self.cond.cond:
      if self.cond.waiting: # read or peek pending
        thread.log("run releases read/peek")
        self.cond.cond.notify() # release the read/peek
        if not self.consume: # peek was pending
          self.cond.wait(thread, "run for read after releasing") # for subsequent read
        else:
          thread.log("run ends")
      else:
        self.cond.wait(thread, "run for read") # for subsequent read/peek

if __name__ == "__main__":
  # tests
  threads = pipethreads()
  r = RexxStage("""
    peekto 'record'
    output record
    readto
    bummer""")
  threads.add(r.start, 'stage')
  threads.add(r.inStreams[0].run, 'run', 'record-1')
  threads.start()
  threads.join(.01)
  #print threads
  #threads.cleanup()

  def dumpLog():
    for r in pipethread.logRecs:
      print r

  dumpLog()

  """ test output
    ('stage', 0.0, ('start',))
    ('run', 0.0, ('run delivers  record-1',))
    ('run', 0.0, ('start wait run for read',))
    Read 'record' peek
    output record-1
    Read  read
    unknown command bummer
    ('stage', 0.0, ('start',))
    ('stage', 0.0, ('start wait peek for run',))
    ('run', 0.0, ('run delivers  record-1',))
    ('run', 0.0, ('run releases read/peek',))
    ('run', 0.0, ('start wait run for read after releasing',))
    ('stage', 0.0, ('end wait peek for run',))
    ('stage', 0.0, ('read releases run',))
    ('run', 0.0, ('end wait run for read after releasing',))
  """
## FO add tests for REXX code in docstring and in file