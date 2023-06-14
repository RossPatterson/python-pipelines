#--------------------- pipetest.py - save this in your python path ---------------------
from  __future__ import with_statement
def foo():
  import test1
import inspect
import PipeExceptions
import re
import stagedict

try:    reload(rexxstages)
except: import rexxstages

try:    reload(parsers)
except: import parsers

try:    reload(stage2)
except: import stage2

try:    reload(basicStages)
except: import basicStages

try:    reload(pipethread)
except: import pipethread

import sys
import time
import types
import threading

"""Considerations:
   Literal1 
"""
def main():
  pipe = PipeLineSet(
    "(end ?) console | locate x | a: dam | console"
    "? readinput | buffer | count | duplicate | a:"
    "? < foo.txt |  b: faninany | console", end='?')
    
# options - global apply to pipelineset, local apply to stage

class PipeAndStageOptions(parsers.SpecificationParser):
  optPat = re.compile(r"\((.*?)\)")
  ## FO replpace re with something simpler
  def __init__(self, spec):
    optionMatch = self.optPat.match(spec)
    if optionMatch: # create and parse options object
      options = optionMatch.groups()[0] # e.g. 'end ?'
      spec = spec[optionMatch.end():]
      self.parseSpec(options)
    self.rest = spec
    
  def setup(self, spec):
    for key, value in spec.items():
      if key in ('escape', 'endchar', 'stagesep', 'separator'):
        if value in "()*.:":
          raise Error, "invalid character %s in %s" % (value, key)
      if key == 'stagesep':
        key = 'separator'
      if 'msglevel' in key or 'no' in key:
        ## almost there - how handle no msglevel??
        msglevel = getattr(self, 'msglevel')
        if 'no' in key:
          setattr(self, 'msglevel', msglevel & value)
        else:
          setattr(self, 'msglevel', msglevel | value) 
      else:
        setattr(self, key, value)

class GlobalOptions(PipeAndStageOptions):
  """applies to the entire pipelineset p 205ff
    end, esc, sep exclude ()*.: """
  BNF = (2,
        "[LISTCMD|LISTERR|LISTRC|MSGlevel snumber|STOP|STOPERROr|TRACE|"
        "MSGlevel snumber|NOMSGlevel snumber|NAME word|"
        "STAGESEP xorc|SEParator xorc|ENDchar xorc|ESCape xorc|PROPAGATeeof]*", 2)
  name = "Global Options"
  listcmd = False
  listerr = False
  listrc = False
  msglevel = 0
  stop = False
  stoperror = False
  trace = False
  name = ""
  separator = "|"
  endchar = escape = None
  propagateeof = False

class LocalOptions(PipeAndStageOptions):
  "applies to one stage p 205ff for keyword meanings" # NO? LISTCMD ?
  BNF = (2, "[LISTCMD|LISTERR|LISTRC|MSGlevel snumber|STOP|STOPERROr|TRACE]*", 2)
  name = "Local Options"
  listcmd = False
  listerr = False
  listrc = False
  msglevel = 0
  stop = False
  stoperror = False
  trace = False

class PipeLineSet:
  simplePipeLines = []
  pipeThreads = []
  trace = True

##  def addsimplePipeLine(self, simplePipeLine):
##    self.simplePipeLines.append(simplePipeLine)
##    simplePipeLine.pipeLineSet = self

  def run(self, records=None, trace=0):
    self.records = records
    if self.errors:
      print "Can't run pipeline due to errors."
      return
    startTime = time.time()
    self.trace = trace
    self.inputRecords = records
    self.runNo += 1
    self.diagnose = trace
    for pipeLine in self.simplePipeLines:
      for stage in pipeLine.stages:
        stage.trace = trace
        stage.initialize()
    # error check
    self.stageThreads = []
    for pipeLine in self.simplePipeLines:
      for stage in pipeLine.startQueue:
        t = threading.Thread(target=stage.start)
        self.stageThreads.append(t)
    for thread in self.stageThreads:
      thread.start()
    # each thread either suspended or stopped?
    # stopped threads return code == 0?
    # delay then check all threads suspended or stopped?
    for thread in self.stageThreads:
      thread.join(0.1)
    for thread in self.stageThreads:
      print thread
    print "Time %s seconds." % (time.time() - startTime, )

  def splitter(self, specsIn, sep, esc):
    """Common routine for splitting a pipeSpec on endchar
       and a simplePipeSpec on separator.
       Handles escape character."""
    collection = []
    if sep:
      specs = specsIn.split(sep)
    else:
      specs = [specsIn]
    itemNo = 0
    for spec in specs:
      itemNo += 1
      # if esc (but not esc esc) at end of spec rejoin with endchar
      while esc:
        if spec[-1] == esc and spec[-2] != esc:
          if itemNo < len(specs):
            spec += sep + specs.next()
          else:
            raise Error, "Escape charater found at end of spec %s", spec
        else:
          break
      collection.append(spec)
    return collection
               
  def __init__(self, pipeSpec=None, **kwargs): # PipeLineSet
    if not pipeSpec:
      return
    self.runNo = 0    
    pipeSpec = pipeSpec.strip()
    self.initalizeQueue = []
    self.errors = []
    self.labels = {}
    
    # create dictionary of alias : stage items
    self.stageModule = basicStages
    self.aliases = dict((name.title(),cls)
                        for name,cls in inspect.getmembers(self.stageModule, inspect.isclass))
    aliasedStages = [i[1] for i in self.stageModule.__dict__.items() if hasattr(i[1], 'alias')]
    for aliasedStage in aliasedStages:
      self.aliases[aliasedStage.alias] = aliasedStage
    
    # Global Options
    self.options = GlobalOptions(pipeSpec)
    pipeSpec = self.options.rest
    self.simplePipeLines = []
    pipsSpecs = self.splitter(pipeSpec, self.options.endchar, self.options.escape)
    for pipeNo, spec in enumerate(simplePipeLines):
      simplePipeLine = SimplePipeLine(self, spec, pipeNo)
      simplePipeLine.pipelineSet = self
      
    if self.errors:
      for error in self.errors:
        print error
    else:
      for pipeLine in self.simplePipeLines:
        for stage in pipeLine.stages:
          stage.finalizeMaster() # check streams for count and connect status, and any customization.
        for error in self.errors:
          print error

  def addError(self, msg, msgNo=0):
    self.errors.append((msg, msgNo))
    print "Error %s %s" % (msgNo, msg)

class StageList(list):
  count = 0
  def __init__(self):
    list.__init__([])
  def add(self, stage):
    self.append(stage)
    self.count += 1
    return self.count
  def __repr__(self):
    return "[" + ", ".join("%s" % (stage.name, ) for stage in self) + "]"
    
class PipeException(Exception): pass
                         
class SimplePipeLine:
  """spec up to an endchar or end of spec"""
  priorStage = None
  initialStage = None
  sepchar = '|'
  
  def addStage(self, stage, priorStage):
    stage.no = self.stages.add(stage)
    stage.addStream('both')
    priorStage.outStreams[-1].send = stage.inStreams[-1].receive
    priorStage.outStreams[-1].nextStream = stage.inStreams[-1]
    stage.inStreams[-1].prevStream = priorStage.outStreams[-1]
    if stage.streamCount == 1:
      stage.currentOut = 1
      stage.currentIn = 1
    return stage  
  
  def __init__(self, pipeLineSet, pipeSpec, no, sub=False):
    ## FO handle leading and trailing connectors when pipe comes from ADDPIPE or CALLPIPE.
    ## FO handle escaped special characters
    self.pipeLineSet = pipeLineSet
    options = pipeLineSet.options
    self.no = no
    self.labels = {}
    self.stages = StageList()
    stageSpecs = pipeLineSet.splitter(pipeSpec, options.sepchar, options.endchar)
    self.startQueue = [] # stages with a start method - run first by dispatcher
    sourceStage = self.stageFactory(stageSpecs[0], 0, 1)
    sourceStage.no = self.stages.add(sourceStage)
    sourceStage.addStream('out')
    sourceStage.currentOut = 1
    for no, stageSpec in enumerate(stageSpecs[1:]):
      sinkStage = self.stageFactory(stageSpec, 1, no + 2)
      if not self.pipeLineSet.errors:
        self.addStage(sinkStage, sourceStage)
        sourceStage = sinkStage
        
  def stageFactory(self, specs, position, no):
    # spec = [labeldef:] [(options)] [stagename [operands]]
    #      = [labelref:]
    
    # label? ---------------------------------------------------
    """+--+-word:----------+--
          +-word.:---------+
          +-word.streamID:-+
       A stream identifier is a word having up to four characters. It cannot be
       a number. Case is respected in stream identifiers. A stream identifier
       made up from lowercase letters is different from one made up of the
       same sequence of characters in uppercase.
       stream: A number or a stream identifier. You can always refer to a particular
       stream by the number (the primary stream is number 0, the secondary
       stream number 1, and so on). Refer to a symbolic identifier instead of
       the stream number if a stream identifier is declared with the label (or
       created with the ADDSTREAM pipeline command).
       stages that explicitly reference streamid: deal, gather"""
    invertOut = False # becomes True if Not precedes real stage

    # Local Options
    options = LocalOptions(specs)
    specs = options.rest
    
    # new stage ------------------------------------------------------
    modStage = None
    invertOutput1 = False # toggled by NOT
    global invertOutput2
    invertOutput2 = False # toggled by leading N
    modified = False
    # (label_ID, options, rest)
    words = specs.split(none, 1)
    word = words[0]
    if word[-1] == ':' and len(word) == 1 or word[-2] != options.escape: # label?
      ## ignores pathological cases!
      label, sep, streamID = word.partition('.') # returns left, sepchar, right
      if label in self.labels:
        self.pipeLineSet.addEerror("Duplicate label %s." % (label, ))
        return
      stage = self.pipeLineSet.labels.get(label, None)
      if stage: # reference
        if options or rest:
          self.pipeLineSet.addError("Text found following label reference.")
          return stage2.NullStage()
        else:
          ## we did this in stage.__init__ stage.addStream("both", streamID)
          return stage
    else:
      label = None
    while True: # stay in loop while encountering modifiers not, casei, zone, ??
      stageName, sep, specs = rest.strip().partition(" ")
      stageName = stageName.title()
      stage, rest = self.stageFactoryCommon(stageName, specs, position)
      if stage:
        if rest: # stage is a modifier
          modified = True
          if stageName == 'Not':
            invertOutput1 = True
          else: # booelan casei, reverse; inputRange zone 
            modStage = stage
        else:
          break
      else: # error
        return stage
    # process the real stage
    stage.options = options
    # apply any modifiers
    stage.modStage = modStage
    stage.invertOutput = invertOutput1 ^ invertOutput2
    if modified:
      stage.getterCls = agents.ModifierGetter
      stage.runnerCls = agents.ModifierRunner
    if label:
      self.pipeLineSet.labels[label] = self.labels[label] = stage
    stage.stageNo = self.stages.add(stage)
    if hasattr(stage, 'initialize'):
      self.pipeLineSet.initalizeQueue.append(stage)
    if position == 0 and not hasattr(stage, 'start'):
      stage.start = stage.run
    if hasattr(stage, 'start'):
      """Call the start method in a thread."""
      self.startQueue.append(stage)
    stage.no = no
    stage.id = "(%s %s) '%s'" % (stage.simplePipeLine.no,  stage.no, stage.name)
    return stage

  def stageFactoryCommon(self, stageName, stageSpec, position=0):
    # stageFactory stuff common to modifier and other stages
    global invertOutput2
    while True:
      stageCls = self.pipeLineSet.aliases.get(stageName, 0)
      if not stageCls: # look for stageName0 or stageName1 (e.g. console0)
        stageCls = self.pipeLineSet.aliases.get(stageName + "01"[position], 0)
        if stageCls: # look for stageName0 or stageName1 (e.g. console0)
          # consider Locate with an alias of Nlocate?
          if stageName[0] == 'N' and issubclass(stageCls, stage2.InvertibleStage):
            invertOutput2 = True # this will be xored with invertOutput1
      if not stageCls: # look for stageName0 or stageName1 (e.g. console0)
        self.pipeLineSet.addError("Class %s not found." % (stageName,))
        return stage2.NullStage(), ''
      break
    stage = stageCls(stageName, self.pipeLineSet, self, stageCls, stageSpec)
    if not stage.parseSpec(stageSpec):
      stage = stage2.NullStage()
    rest = "" ## stage.parsers.parse(stageSpec) 
 ##   if error: ## enhance self.pipeLineSet.error()
 ##     self.pipeLineSet.errors.append("Error initializing stage %s '%s': %s" %
 ##                                    (stage.stageNo, stageSpec, error))
    return stage, rest

if __name__ == "__main__":
  main()
  
"""
SEVER: Detach a stream* from the program. The other side of the connection sees end-of-file.
       Under addpipe sever also restores the stacked connection.
       In a stage with more than one output stream, sever a stream as soon as you
       have finished writing to it. This may avoid a stall
*Note: Though much CMS/TSO Pipelines documentation speaks of severing a stream
rather than severing the connection to a stream, it is understood that the severance occurs
by removing the connector between the stream being severed and the stream it is
connected to, if any. Streams are created by the pipeline specification parser and by the
pipeline command ADDSTREAM; once created a stream exists as long as the stage to which
it is attached. There is no pipeline command to destroy a stream.

End-of-file: (under addpipe) on the new connection sets return code
             12 in a READTO or PEEKTO pipeline command.
  Count: The pipeline dispatcher resumes count with a return code to indicate end-of-file.

Stall: To stall, a pipeline must also have more than one path between some stages.
    In general, you must consider the possibility of the pipeline stalling when there are one or
    more meshes in the pipeline topology; that is, when the pipeline contains two stages
    between which there is more than one path.
  result of

usage:
1) Save this module (pipetest.py) in your python path.
2) Save the test1 module in your python path and run it.

 Pipe() creates an instance of each stage's class, gives it primary input and output
   streams and calls its initialize method.
 Connects first stage's primary output stream to 2nd stage's primary input atream,
   2nd stage's primary output stream to 3rd stage's primary input atream, ....
 After all stages initialized report any errors. If no errors
 (FO call each stage's finalize method to check streams and connections)
 After pipe is created, call its run method which run a stage from the outerQueue.
 Events ensue as getter and runner objects are queued then called.

Example of tracing (from a JH email): 

 literal|(trace nomsg 15) fanout|hole
 PIPDSQ028I Starting stage with save area at X'03E70C08 03E56EB0 00000000' on commit level -2.
 PIPDSQ001I ... Running "fanout".
 PIPDSP537I Commit level 0.
 PIPDSQ031I Resuming stage; return code is 0.
 PIPDSP034I "Locate" called.
 PIPDSQ031I Resuming stage; return code is 0.
 PIPDSP036I Select OUTPUT stream 0.
 PIPDSQ031I Resuming stage; return code is 0.
 PIPDSP035I Output 0 bytes.
 PIPDSP039I ... Data: "".
 PIPDSQ031I Resuming stage; return code is 0.
 PIPDSP033I Input requested for 0 bytes.
 PIPDSQ031I Resuming stage; return code is 0.
 PIPDSP036I Select OUTPUT stream 0.
 PIPDSQ031I Resuming stage; return code is 0.
 PIPDSP034I "SHORT" called.
 PIPDSQ031I Resuming stage; return code is 0.
 PIPDSP020I Stage returned with return code 0.
       Ok.  08:48:32   08/22/08   0   0.001 ...pa2  
"""
#--------------------- end pipetest ---------------------