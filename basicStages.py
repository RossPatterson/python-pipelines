# --------- STAGES ----------

try:    reload(rexxstages)
except: import rexxstages

try:    reload(stage2)
except: import stage2

try:    reload(threading)
except: import threading

class Append(stage2.Stage):
  ## NEEDS WORK
  """copy records from its primary input stream to its primary
  output stream, and then invoke another stage or subroutine pipeline and write the   
  records produced by that stage or subroutine pipeline to its primary output stream
  (if it is connected)."""
  def setup(self):
    """options [stage [operands] or subroutine pipeline]
     stage
       is the name of a built-in stage or the name of a user-written stage. If you
       specify a user-written stage, it must either have a file type of REXX or it must
       be invoked through the REXX stage. The stage must be a stage you can
       specify as a first stage in a pipeline.
     operands
       are any operands valid for the specified built-in stage or user-written stage.
     subroutine
       is a subroutine pipeline. No explicit connector can be specified. The beginning
       of the subroutine pipeline is not connected. The output stream from the last
       stage in the subroutine pipeline is connected to APPEND's primary output stream.
    see reference regarding 2-pass scanning & treatment of special characters"""
  def run(self):
    self.output[0](record)
  def sever(self):
    # run the stage or subroutine pipeline & write resultant records to output[0]
    self.output[0].sever()

class Beat(stage2.Stage):
  ## NEEDS WORK
  """beat is used as a "heartbeat monitor". It passes records
     from its primary input stream to its primary output stream.
     If no input record arrives within the specified interval,
     a record is written to the secondary output stream. A
     record can be written to the secondary output stream once
     per input record or each time the interval expires.
     >>--BEAT---+------+-+-number--------+-+-----------------+--><
                +-ONCE-+ +-number.number-+ +-delimitedString-+"""
 ## def __init__(self, options):
 ##   self.setupOptionsParser(1, "[ONCE]? decimalnumber [delimitedstring]?")
  ## experiment using re to parse specs
  ## works nicely here as specs are simple, with no abbreviations
  import re
  pat = re.compile(r'(once)?\s*(\d*?\.\d*)\s*(\S*)$', re.I)
  def setup(self, spec):
    specs = self.pat.findall(spec)[0] # [once] dd[.dd] [delstr]
    if len(specs) != 3:
      return "One to three spec words expected."
    self.repeat = not specs[0]
    try:
      self.interval = float(specs[1])
      if self.interval <= 0:
        return "Interval must be > 0." 
    except:
      return "Interval must be integer or float >= 0."
    self.string = specs[2]
  def initialize(self):
    import threading 
    self.timer = threading.Timer(self.interval, self.beat)
    self.timer.start()
  def run(self, record): # we've received a record!
    self.timer.cancel()
    self.output0(record)
    self.initialize() # restart timer
  def beat(self): # interval expired w/no record
    if len(self.outstreams) == 2 and self.outstreams[1].connected:
      self.output1(self.string)
      if self.repeat:
        self.initialize()
    else: # can't write secondary 
      self.exit(0)

class Buffer(stage2.Stage):
  def setup(self, spec=None):
    self.buffer = []
  def run1(self, record):
    self.buffer.append(record)
    self.log("buffered '%s'" % (record,))
  def eof1(self):
    for record in self.buffer:
      self.output1(record)
    self.sendeof1()
    
class PassRec(rexxstages.RexxStage):
  """/* rexx test */
     peekto 'record'
     output record
     readto """

class Casei(stage2.ModifierStage):
  ## NEEDS WORK
  """The argument to casei is a stage to run. casei invokes the
     specified stage having translated the argument string to
     uppercase. It then passes input records translated to uppercase
     to this stage. When the stage writes a record to its primary
     output stream, the corresponding original input record is written
     to the primary output stream from casei; likewise, the original
     input record is written to the secondary output stream, when the
     stage writes to its secondary output stream. The argument is
     assumed to be a selection stage; that is, it should specify a
     program that reads only from its primary input stream and passes
     these records unmodified to its primary output stream or its
     secondary output stream without delaying them.
     
     >>--CASEI--+----------------------+--+---------+---string--><
                +-ZONE--| inputRange |-+  +-REVERSE-+
       where string is the "real" stage.

     The IBM version constructs a subroutine pipeline to perform the
     required transformation on input records. The Python version
     instead uses a special runner agent to apply the tranforms and
     save the original record, and a special getter agent to retriev
     the original record."""
    
class Chop(stage2.Stage):
  ## NEEDS WORK
  """                  +-80-----+
     >>--+-CHOP-----+--+--------+--><
         +-TRUNCate-+  +-number-+
         
     >>--+-CHOP-+------------+--+------------------------+-+-----+--| target |-><
                +-ANYCase----+  |             +-BEFORE-+ | +-NOT-+
                +-CASEANY----+  +---------+---+--------+-+
                +-IGNORECASE-+  +-snumber-+   +-AFTER--+ 
                +-CASEIGNORE-+ """
                              
##  Options.setupOptionsParser(1, "[80!number] [!ANYCase|CASEANY|IGNORECASE|CASEIGNORE] "
##                "[snumber [BEFORE!AFTER]] NOT? target")
  alias = "TRUNCate"
  number = 80
  def setup(self, specs):
    if specs:
      self.number = int(specs)
    def run(self, record):
      self.output0(record[:self.number])
      self.output1(record[self.number:])
    
class Console0(stage2.Stage):
  """When first in simplePipeLine"""
  def run(self):
    self.log("awaits input")
    while True:
      try:
        record = raw_input()
      except KeyboardInterrupt:
        self.sendeof1()
        break
      else:
        if record:
          self.output1(record)
        else:
          self.sendeof1()
          break
    
class Console1(stage2.Stage):
  """When not first in simplePipeLine"""
  def setup(self, id=None):
    self.id = id
  def run1(self, record):
    print "%s writes '%s'" % (self.id, record)
    self.output1(record)
    
class Copy(stage2.Stage):
  """copy passes the input to the output in a way that can delay by one record. It may be
     useful to avoid a stall in a pipeline network where a one-record
     delay is sufficient to prevent the stall.  """
  def start(self):
    self.instream1.wait()
    self.output1(self.record)
  def run(self, record):
    ## THIS DOES NOT EXACTLY DO IT but it may suffice!
    self.record = record
    self.instream1.set()
    
class Count(stage2.Stage):
# there is a much better version of Count (and other stages) in module ??
  def setup(self, spec=None):
    self.total = 0
    self.count = lambda rec, tot: tot + len(rec)
  def run1(self, record):
    self.log("counted '%s'" % (record,))
    self.total = self.count(record, self.total)
  def eof1(self):
    self.output1(str(self.total))
    self.sendeof1()

class Count(stage2.Stage):
  class Counter():
    """an instance handles one of CHARACTErs WORDS LINES MINline MAXline"""
    def __init__(self, func, label, total=0):
      self.total = total
      self.func = func
      self.label = label
    def run(self, record):
      self.total = self.func(record, self.total)
    def __repr__(self):
      msg = str(self.total)
      if self.label: msg += " " + self.label
      return msg
  CounterClass = Counter
  streamSpecs = "!~ !+"
  opts = {
    'characters' : (lambda rec, tot: tot + len(rec), 0),
    'words' :      (lambda rec, tot: tot + len(rec.split()), 0),
    'lines' :      (lambda rec, tot: tot + 1, 0),
    'minline' :    (lambda rec, tot: min(len(rec), tot), 999999999),
    'maxline' :    (lambda rec, tot: max(len(rec), tot), 0),
    'for someting more complex' : (complex, 0), }
  labels = False

  """Records are read from the primary input stream; no other input stream
  may be connected. When the secondary output stream is not defined, input
  records are discarded; the record containing the counts is written to the
  primary output stream. When the secondary output stream is defi ned, the
  input records are copied to the primary output stream; the primary output
  stream is severed at end-of-file on the primary input stream; and the
  record containing the counts is then written to the secondary output stream.
               v--------------+
    >>--COUNT--+---CHARACTErs-+---><
               +- WORDS-------|
               +- LINES-------|
               +- MINline-----|
               +- MAXline-----+
    The keywords can be expressed thus: [CHARACTErs|WORDS|LINES|MINline|MAXline]+
    [] = multiple choice
    +  = at least one
    |  = choice separator
    leading caps = minimum abbreviation
  stage.parserFactory translates BNF into parser objects that
  handle the stage options. These include a kwdDict. When given a
  series of words a parser returns the corresponding values or reports
  problems. It may be the case that some words will be followed by
  related words. A stage will have a sequence of parsers, each of
  which handles what it can then ends."""
  BNF = (4, "[CHARACTErs|WORDS|LINES|MINline|MAXline]+", 1)
  
  def setup(self, specs):
    # called by masterSetup after that deals with parsing the specs
    self.counters = []
    self.runners = []
    errors = []
    for spec in specs:
      func, initTotal = Count.opts.get(spec, 0)
      if func:
        ctr = Count.CounterClass(func, spec, initTotal)
        self.counters.append(ctr)
        self.runners.append(ctr.run)
      else:
        errors.append("Invalid option %s" % (spec))
    if errors:
      return errors
  def finalize(self):
    """Ensure instreams[0].connected, outstream[0].connected
       Allow outstreams[1].connected and switch record flow
    """
    self.secondary = len(self.outstreams) == 2
  def complex(self, record):
    "model for counting functions that are more complex than one expression"
  def initialize(self):
    for counter in self.counters:
      counter.total = 0
  def run(self, record):
    for method in self.runners:
      method(record)
    if self.secondary:
      self.output0(record)
  def eof(self):
    totals = [str(ctr.total) for ctr in self.counters]
    self.output(" ".join(totals), streamNo=self.secondary) # put total(s) in outstream
  ## FO should there be a method that responds to the Get EOF > agent to exit?    
    self.exit(0)

class Dam(stage2.Stage):
  """dam waits for the first record to arrive on its primary input stream."""
  burst = False
  def setup(self, spec=None):
    self.event = threading.Event()
  def run1(self, record): # record arrives on primary 
    """Unblock.
       Short self to output1.
       Then stream1 ..."""
    self.log('1 received ' + record)
    self.output1(record)
    if not self.burst:
      self.event.set() # resumes the secondary input thread
      self.log('1 event cleared')
      self.burst = True
  def run2(self, record): # record arrives on secondary 
    """Block until record arrives on primary"""
    if not self.burst:
      self.log('2 received ' + record + ' and waiting')
      self.event.wait() # suspends the thread leading to the secondary input
      self.log('2 wait over')
    self.output1(record)
  def eof2(self): # this could be handled by a smart scanner!
    self.output1('eof')
      
class Deal(stage2.Stage):
  """Pass Input Records to Output Streams Round-robin."""
  def initialize(self):
    self.outstreamNo = 0
  def run(self, record):
    self.outstreams[self.outstreamNo].output(record)
    self.outstreamNo += 1
    if self.outstreamNo == len(self.outstreams):
      self.outstreamNo = 0
      
class Delay(stage2.Stage):
  """delay copies an input record to the output at a particular time
     of day or after a specified interval has elapsed. The first word
     of each input record specifies the time at which it is to be
     passed to the output."""
  def run(self, record):
    # compute interval (seconds) from first word of record (and time of day?)
    if interval > 0:
      self.instream.event.wait(interval) # delay output
    self.output0(record)
  def terminate():
    self.timer.cancel()
    self.instream.event.set() # consume input record
   
class Duplicate(stage2.Stage):
  def setup(self, spec=None):
    spec = spec if spec else '1'
    self.spec = int(spec) + 1
  def run1(self, record):
    for i in range(self.spec):
      self.output1(record)
  
class Elastic(stage2.Stage):
  ## NEED HELP WITH instream.eof?
  """elastic reads records from the input into a buffer and writes records
     from this buffer to the output in a way that does not prevent it
     from reading another record while it is writing a record.

     elastic may be used to avoid a stall in a multistream network.

     When elastic has two input streams, the secondary input stream is
     assumed to be a feedback from the stages connected to the primary
     output stream.

     When the secondary input stream is not defined, elastic reads
     records as they arrive and writes them as they are consumed. It
     tries to minimise the number of records buffered inside.

     When the secondary input stream is defined, elastic first passes
     the primary input stream to the primary output stream, buffering
     any records it receives on the secondary input stream. When the
     primary input stream is at end-of-file, elastic enters a
     listening mode on the secondary input stream. As long as it has
     records buffered, it writes to the primary output stream and
     reads what arrives at the secondary input stream and stores it in
     the buffer. elastic flushes its buffer and terminates when the
     secondary input stream reaches end-of-file. elastic also
     terminates when the buffer is empty and there is no input record
     available after it has suspended itself to let all other ready
     stages run. At this point there should be no further records in
     the feedback loop; elastic terminates, because reading a further
     record would be likely to cause a stall.  """
  def start(self):
    while not self.instream.eof:
      for record in self.buffer:
        self.output0(record)
      self.instream.event.wait()
  def run(self, record):
    self.buffer.append(record)
    self.instream.event.set()
    
class Fanin(stage2.Stage):
  ## NEEDS WORK
  inputRange = (1,9999)
  """FANIN, without operands, reads all records from all of its connected
     input streams starting with the primary input stream and
     continuing with the rest of the streams in order of increasing
     stream number. If one streamnum or streamid is specified, FANIN
     reads all the records from only that input stream. If multiple
     operands are specified, FANIN reads records from all the
     identified streams in the order specified.

     FANIN reads all the records from a stream before it begins
     reading records from the next stream.

     Primary Output Stream: FANIN copies its input records to its primary output
     stream in the same order it reads the input records.
     >>--FANIN--+-------------+--><
                + v---------+ |
                +-+-stream--^-+  """

  def setup(self, specs):
    """create list of streams from which to read"""
    self.streamListIn = specs # list; could be empty

  def finalize(self):
    if self.streamListIn:
      for stream in self.streamListIn:
        if isdigit(stream): # translate streamid to stream
          try:
            stream = self.streamsByID[stream]
          except KeyError:
            raise Error, "Invalid streamid '%s'." % stream
    else:
      self.streamListIn = self.instreams[:]
      
  def initialize(self):
    # block all but first input
    for stream in self.streamList[1:]:
      stream.block()
    self.streamList.pop(0)
    self.commitLevel = 0
    self.instreams[0].eof = self.eof

  def eof(self):
    ## complete this
    ## provide a stack to save the original eof?
    if self.streamList:
      self.eof = xxx
      next = self.streamList[1].pop(0)
      next.unblock()
      next.eof = self.eof
    else:
      self.exit()

  def run(self):
    # move record from current input to primary output
    # when severed increment current input
    # when last input severed, sever output
    if self.severed:
      if self.streamList:
        self.streamList.pop(0).unblock()
      else:
        self.output[0]()
    else:      
      self.output[0](record)
      
class Faninany(stage2.Stage):
  def setup(self, spec=None):
    self.spec = spec # could be STRICT
  def runany(self, record):
    self.output1(record)

class Fanout(stage2.Stage):
  ## NEEDS WORK
  """write record from instream to all outstreams"""
  def run(self, record):
    for stream in self.output:
      stream(record)
  def sever(self):
    """sever all outstreams when instream is severed"""
    for stream in self.output:
      stream.sever()

class FanoutTwo(stage2.Stage):
  ## NEEDS WORK
  """fanoutwo is a specialised version of fanout designed to create a stream
     that can be passed to a device driver. Unlike a device driver, it
     propagates end-of-file backwards from the primary output stream
     to the primary input stream.

     Operation: fanoutwo passes the input record first to the primary
     output stream and then to the secondary output stream. It
     terminates when it receives end-of-file on the primary output
     stream; it shorts the primary input stream to the primary output
     stream when it receives end-of-file on the secondary output
     stream."""
  def run(self, record):
    ## next stages can sever their inputs. outputx then returns True 
    if self.output0(record):
      self.exit()
      return
    if self.output1(record):
      self.short()
  ## short should replace instreamx[0].run with output0

##class Dispatcher(stage2.Stage):
##  ## DO WE NEED?
##  """Each simplePipeLine starts with an instance."""
##  def run(self):
##    """Start any stages with a start method.
##       Then call output1 to run the 1st stage."""
##    for stage in self.simplePipeLine.startQueue.reversed():
##      stage.start()
##    self.output1('')

class Hole(stage2.Stage):
  ## NEEDS WORK on the eof handling
  """hole reads and discards records without writing any. It can be
     used to consume output from stages that would terminate
     prematurely if their output stream were not connected.

     hole reads from all defined input streams; it does not write
     output. The output streams remain connected until hole reaches
     end-of-file on all its input streams."""

  def run(self, record):
    pass

class Join(stage2.Stage):
  ## NEEDS WORK
  """join puts input records together into one output record,
     inserting a specified string between joined records. All input
     lines are joined into one output record when an asterisk is
     specified. The maximum length of an output record can also be
     specified."""
  #  Concatenates one or more input records into a single output
  #  record. JOIN reads n+1 records from its primary input stream, concatenates the
  #  records (with string between), and writes the concatenated records to its primary output stream, if it is
  #  connected. Records are concatenated in the order in which they appear in the
  #  primary input stream.
  def setup(self):
    ## this is an exercise in parsing optionis. Its been a while....
    # JOIN [1| |n|*] [ |/|X|H|B string] [ |maxlength]
    # save n, string, maxlength
    self.buffer = []
    self.count = 0
    self.n = 1
    self.string = ""
    self.maxlength = None
    self.length = -len(self.string)
  def run(self, inStream):
    if self.count > self.n:
      candidate = self.string.join(self.buffer)
      if self.maxlength: candidate = candidate[:self.maxlength]
      self.output[0].write(inStream, candidate)
      self.buffer = [inStream.record]
      self.count = 0
      self.length = -len(self.string)
    else:
      if self.maxlength:
        self.length += len(inStream.record) + len(self.string)
        if self.length < self.maxlength:
          self.buffer.append(inStream.record)
      else:
        self.buffer.append(inStream.record)
      self.count += 1

class Literal0(stage2.Stage):
  def setup(self, spec=''):
    self.spec = spec
  def start(self):
    self.output1(self.spec)
    self.exit
    
class Literal1(stage2.Stage):
  """This stage requires a start method
     since it is NOT first in a pipeline
     and must generate output independent of any input.
     Also must block primary input until initial output completed."""
  state = 0 # no output 
  def setup(self, spec=''):
    self.spec = spec
  def start(self):
    self.output1(self.spec)
    if self.state == 1: # primary blocked
      self.event.set() # unblock the primary input thread
      self.log('event cleared')
    self.state = 2 # primary open
  def run1(self, record):
    if self.state == 0: # no output 
      self.log('received ' + record + ' and waiting')
      self.state = 1 # primary blocked
      self.event.wait() # suspends until stage started
      self.log('wait over')
    self.output1(record)
  ## FO point run1 to this after initial output
  def runAgain(self, record):
    self.output1(record)
    
class Locate(stage2.Stage, stage2.InvertibleStage):
  ## NEEDS WORK
  invertible = True
  """--[N]LOCATE-+---------+-+-------+-+-------------+-+-------+-+-----------------+
                 +-ANYcase-+ +-MIXED-+ +-inputRanges-+ +-ANYof-+ +-delimitedString-+
                             +-ONEs--+
                             +-ZEROs-+  """
  alias = 'NLOCATE' 
  streamSpecs = '!! ~?' # primary in & out required, secondary out optional
  BNF = (1, "[ANYcase]? [MIXED|ONEs|ZEROs]? [inputranges]? [ANYof]? delimitedstring", 1)

  def setup(self, spec=''):
    self.spec = spec
  def run1(self, record):
    if self.spec in record:
      self.output1(record)
    else:
      self.output2(record)
    
class Lookup(stage2.Stage):
  ##  >>--LOOKUP--+-------+--+-----------+--+----------+--+------------+------> 
  ##              +-COUNT-+  +-INCREMENt-+  +-SETCOUNT-+  +-TRACKCOUnt-+        
  ##                                                                            
  ##     +-NOPAD-----+                                                          
  ##  >--+-----------+--+---------+--+---------------------+--+---------+-----> 
  ##     +-PAD--xorc-+  +-ANYcase-+  +-AUTOADD--+--------+-+  +-KEYONLY-+       
  ##                                            +-BEFORE-+                      
  ##                                                                            
  ##  >--+--------+--+----------------------------+---------------------------> 
  ##     +-STRICT-+  +-inputRange--+------------+-+                             
  ##                               +-inputRange-+                               
  ##                                                                            
  ##     +-DETAIL--MASTER----------------------+                                
  ##  >--+-------------------------------------+----------------------------->< 
  ##     +-DETAIL------------------------------+                                
  ##     +-DETAIL--ALLMASTER--+----------+-----+                                
  ##     |                    +-PAIRWISE-+     |                                
  ##     +-MASTER--+--------+------------------+                                
  ##     |         +-DETAIL-+                  |                                
  ##     +-ALLMASTER--+----------------------+-+                                
  ##                  +-DETAIL--+----------+-+                                  
  ##                            +-PAIRWISE-+
  BNF = (1, '', 1)
  def setup(self, values):
    self.reference = {}
  def sever(self, inStream):
    if inStream.streamNo == 1: # all masters in; unblock primary inStream
      self.inputs[0].blocked = False
    elif inStream.streamNo == 0: # all details in; sever outStreams
      # if tertiary, dump unmatched masters
      pass
  def run1(self, record):
    key = '' ## derive from InputRange1
    self.reference[key] = [record, False]
  def run0(self, record):
    key = '' ## derive from InputRange2
    if key in self.reference:
      record = self.reference[key][0]
      self.output[0].write(record)
      self.reference[key][1] = True
    else:  
      self.output[0].write(inStream)
  """ LOOKUP matches records in its primary input stream with records in its secondary input stream
  and writes matched and unmatched records to different output streams.
  Whole contents of records are matched by default, or the records are matched on the basis of a key field
  (the contents of a specified range of columns in the records).

  Before finding records, LOOKUP builds the reference. It does so by reading all records on the
  secondary input stream into a buffer (called the reference). These records are called master records.
  LOOKUP discards master records with duplicate keys while loading the buffer.

 After building the reference, LOOKUP reads records from its primary input stream
 and looks for a matching record in the set of reference records. The records read
 from the primary input stream are referred to as detail records. By default, entire
 records are compared, but you can specify column ranges to look for a key.

  Upon finding a match, LOOKUP, by default, writes the detail record and the
  matching master record to its primary output stream. Use the operand DETAILS to get
  just the detail records. If a detail record does not have a matching master record,
  LOOKUP writes the detail record to its secondary output stream.

  After processing all the detail records, LOOKUP writes all unreferenced master records
  to its tertiary output stream. By unreferenced we mean those not matched by at least
  one detail record. LOOKUP writes the unreferenced records in ascending order by their keys.
  """

class Not(stage2.ModifierStage):
  ## NEEDS WORK
  ## we could use callpipe (as does CMS version)
  ## or set a flag that tells the parser to reverse the outputs
  ## and drop the Not
  """>>--NOT--word--------------><
                    +-string-+
     Not runs the stage specified (most often a selection stage) with
     its output streams inverted. The primary output stream from the
     stage is connected to the secondary output stream from not (if it
     is defined). The secondary output stream from the stage is
     connected to the primary output stream from not. The stage must
     support two output streams.

     The specified stage is run in a subroutine pipeline. Records are
     read from the primary input stream; no other input stream may be
     connected. The primary input stream is connected to the primary
     input stream of the stage. The primary output stream from the
     stage is connected to not's secondary output stream if one is
     defined. The secondary output stream from the stage is connected
     to not's primary output stream."""

class Pad(stage2.Stage):
  ## NEEDS WORK
  """Extends records with one or more specified characters or blanks. You can extend a record on the
     left or the right. PAD reads records from its primary input stream, extends the records, and
     then writes the resulting records to its primary output stream, if it is connected."""
  ## operands side (Left or Right) number-of-chars padchar (BLANK (default) or char or hexchar)
  def initialize(self):
    ops = self.operands.split()
    if ops[0].isalpha(): # check for Left or Right
      self.side = ops[0].upper()
      if "LEFT".startswith(self.side) or "RIGHT".startswith(self.side):
        self.side = self.side[:1] # capture just the first letter
      else:
        self.error("First operand %s should be Left or Right or integer." % self.side)
      del ops[0] # remove operand from operand list
    else:
      self.side = "R" # the default
    if self.side == "R":
      self.just = str.rjust
    else:
      self.just = str.ljust
    try:
      self.amount = int(ops[0]) # now expecting a non-negative integer
    except:
      self.error("Operand %s is not an integer." % ops[0])
    if self.amount < 0:
      self.error("Operand %s is negative." % ops[0])
    del ops[0] # remove operand from operand list
    if ops: # 3rd operand is char or hexchar
      self.char = ops[0]
      if len(self.char) == 2:
        try:
          self.char = chr(int(self.char,16))
        except:
          self.error("Invalid hex char %s." % self.char)
      elif len(self.char) != 1:
        self.error("Invalid char length %s." %self.char)
                    
  def run(self):
    self.output(self.just(record, self.amount, self.char))

class ReadFile(stage2.Stage):
  alias = '<'
  def setup(self, spec=None):
    self.fn = spec
  def initialize(self):
    try:
      self.file = open(self.fn)
    except IOError, rest:
      return rest
  def run(self):
    for record in self.file:
      if record[-1] == '\n':
        self.output1(record[:-1])
      else:
        self.output1(record)
    self.sendeof1()
    self.file.close()
    
class ReadInput(stage2.Stage):
  def start(self):
    for record in self.pipeLineSet.records:
      self.output1(record)
    self.sendeof1()

class Reverse(stage2.Stage):
  def run(self, record):
    self.output0(record[::-1])

class Sort(stage2.Stage):
  ## NEEDS A LOT OF WORK
  """                  +-NOPAD-----+
  >>--SORT-+--------+--+-----------+-+---------+----->
           +-COUNT--+  +-PAD--xorc-+ +-ANYcase-+
           +-UNIQue-+
           
     +-Ascending-------------------------------------+
  >--+-----------------------------------------------+--><
     +-Descending------------------------------------+
     | v------------------------------------------+  |
     | |            +-Ascending--+                |  |
     + +-inputRange-+------------+-+-----------+--^--+
                    +-Descending-+ +-NOPAD-----+
                                   +-PAD--xorc-+ 
  sort reads all input records and then writes them in a specified order."""

class Specs(stage2.Stage):
  """        +-STOP--ALLEOF-----+ v---------------------+
  >>--SPECs--+-----------------+--+-+-| field |------+--^-><
             +-STOP--+-ANYEOF--+    +-READ-----------|
                     +-number--+    +-READSTOP-------|
                                    +-WRITE----------|
                                    +-SELECT--stream-|
                                    +-PAD--xorc------+
field:
|--+----inputRange------------------------------+--+-------+-->
   +-NUMBER--+---------------+-+-------------+--| -+-STRIP-+
   |         +-FROM--snumber-+ +-BY--snumber-+
   +-TODclock--------------------------------|
   +-dstring---------------------------------+
   
> --------------------------Next-------(1-)------------------->
+-| conversion +-+ | +-NEXTWord-+ +-.--number-+|
+-number------------------------|
+-range-------------------------+
>--------------|
+-Left---|
+-Centre-|
+-Right--+
conversion:
+----f2t-------------(2-)-|
+-P2t(snumber)-|
+-f2P(snumber)-+
  """
  abbrev = 'spec'
  BNF = (1, " ", 1)

class Split(stage2.Stage):
  ## OLD MODEL
  # >= 1 record output per run
  def setup(self, spec):
    self.splitChars = spec
  def run(self, record):
    self[:] = record.split(self.splitChars)
    self.enqueueGetter()
##    return self.PEEK
  def get(self): 
    for record in self:
      yield record
    else:
      self.readto()

class Synchronise(stage2.Stage):
  alias = 'synchronize'
  ## NEEDS WORK
  ##  How to handle abbreviated stage names
  """Synchronise Records on Multiple Streams
     synchronise forces records on parallel streams of a pipeline to
     move in unison through the pipeline. synchronise waits until
     there is a record available on every input stream and then copies
     one record from each input stream to the corresponding output
     stream. It copies no further records to its output until there is
     again a record available on each input stream. With synchronise,
     the records on one stream can be used to pace the flow through
     the pipeline of the records on some other stream.
     >>--+-SYNChronise-+---><
         +-SYNChronize-+"""
  ## def finalize(self):
    ## THIS HAPPENS BY DEFAULT??
    ## for instream in self.instreams:
    ##  instream.run = self.run
  def initialize(self):
    self.recordCount = 0
  def run(self, record):
    self.recordCount += 1
    if self.recordCount < len(self.instreams):
      # suspend until all runs have been called
      self.instream.event.wait()
    else:
      for instream in self.instreams:
        instream.event.set()
    self.output(record)
    
class Take(stage2.Stage):
  ## NEEDS WORK
  """take FIRST selects the first n records and discards the remainder.
     take LAST discards records up to the last n and selects the last n records.
            +-FIRST-+  +-1------+
  >>--TAKE--+-------+--+--------+-+-------+--><
            +-LAST--+  +-number-| +-BYTES-+
                       +-*------+

When BYTES is omitted, take FIRST copies the specified number of
records to the primary output stream (or discards them if the primary
output stream is not connected). If the secondary output stream is
defined, take FIRST then passes the remaining input records to the
secondary output stream.

take LAST stores the specified number of records in a buffer. For each
subsequent input record (if any), take LAST writes the record that has
been longest in the buffer to the secondary output stream (or discards
it if the secondary output stream is not connected). The input record
is then stored in the buffer. At end-of-file take LAST flushes the
records from the buffer into the primary output stream (or discards
them if the primary output stream is not connected)."""
  import re
  pat = re.compile(r'(first|last)?\s+(\d+)?\s+(bytes)?', re.I)
  def setup(self, spec):
    match = self.pat.findall(spec)
    if match and len(match==3):
      end, count, unit = match
    else:
      return "Invalid stage specification."
  def run(self):
    pass
  def get(self):
    pass
  def eof(self):
    pass

class OutputToFile(stage2.Stage):
  """superclass of next two"""
  def setup(self, spec=None):
    self.fn = spec
  def initialize(self):
    try:
      self.file = open(self.fn, self.mode)
    except IOError, err:
      return err
  def run(self, record):
    self.file.write(record + '\n')
  def terminate(self):
    self.file.close()
  
class AppendFile(OutputToFile):
  alias = '>>'
  mode = 'a'

class WriteFile(OutputToFile):
  alias = '>'
  mode = 'w'

class Zone(stage2.ModifierStage):
  ## NEEDS WORK
   """messy syntax"""
   
# This is an example of a user-written stage (CMS Pipelines has no SHORT stage)
class Short(stage2.Stage):
  """pass records that are shorter than the length specified"""
  def setup(self, spec="10"):
    try:
      self.length = int(spec)
    except:
      return "'%s' is not an integer." % spec
  def run(self, record):
    if '$$' in record: # example of runtime error handling
      return 'Illegal dipthong $$ in record.'
    if len(record) < self.length:
        self.output0(record) # put record in outstream 
