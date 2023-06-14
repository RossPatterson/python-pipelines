# pipeCommands module

# goals
#  provide methods for the Stage class that implement the pipeline commands
#  provide access to the BNF parser for commands marked with *
#  have all commands in place but minimize setup overhead as it is likely that
#  most commands will not be used in a particular pipe.
"""
Transport Data
BEGOUTPUT Enter implied output mode.
READTO Read a record from the currently selected input stream.
PEEKTO Preview the next record from the currently selected input stream.
  The record stays in the pipeline.
OUTPUT Write the argument string to the currently selected output stream.
SHORT Connect the currently selected input stream to the currently selected
  output stream. The streams are no longer connected to the program.

Control Pipeline Topology
ADDPIPE Add a pipeline specification to run in parallel with your program.
  This is used, for instance, to replace the current input stream with a file to imbed.
ADDSTREAM* Add an unconnected stream to the program.
CALLPIPE Run a subroutine pipeline.
SELECT* Select an input or output stream, or both. Subsequent requests for
  data transport are directed to this stream.
SEVER* Detach a stream from the program. The other side of the connection sees end-of-file.

Control Programs
COMMIT* Commit the program to a particular level. The return code is the
  aggregate return code for the pipeline specification so far.
EOFREPORT* Modify the return codes reported by the data transport commands.
NOCOMMIT Disable automatic commit first time a program performs an I/O operation.
REXX Call a subroutine REXX program. The program has access to the pipeline
  command environment and the caller's streams.
SETRC* Set the return code for the program writing a record to the currently selected input stream.
SUSPEND Allow other stages to run.

Scanning
GETRANGE* Extract part of record or string.
SCANRANGE* Parse an inputRange.
SCANSTRING* Parse an delimitedString.

Issue Messages
MESSAGE Write the argument string as a message.
ISSUEMSG* Issue a CMS/TSO Pipelines message. The argument specifies a
  message number; the message text is obtained from a message table.

Query Program's Environment
MAXSTREAM* Return the highest-numbered stream.
RESOLVE Return entry point address for a pipeline program.
STAGENUM Return position in pipeline.
STREAMNUM* Return stream number corresponding to an identifier.
STREAMSTATE* Return connection status of stream."""
  
# NEW
# these functions are added to the Stage class methods; thus the first argument is the stage
import streams2
def newaddStream(stage, spec):
  # pattern for a function that accesses a parser
  # which is generated/retrieved at first call and whose work is done
  # by a nested function therefore minimizing startup overhead

  def do(stage, parsedSpec):
    print(stage.name, parsedSpec)
  
  parse("ADDSTREAm", (1, "[BOTH||INput|OUTput] [|streamID]", 1), do, stage, spec)


# CURRENT
def addStream(stage, side="both", streamID=None):
  """Implements the pipeline command:
                 +-BOTH---+
  >>--ADDSTREAm--+--------+-+-----------+-><
                 +-INput--+ +-streamID-+
                 +-OUTput-+
     Used also by stageFactory when creating a stage
     and when handling a label reference. """
  ## FO connect pipeline commands to BNF parser?

  def addOneStream(stage, container, cls, streamID):
    if len(container) == stage.maxStreams:
      stage.pipeLineSet.addError("Too many streams.")
    stream = cls(container, stage, streamID)

  stage.streamCount += 1
  side = side.lower()
  specOK = False
  if side in ("in", "both"):
    addOneStream(stage, stage.inStreams, streams2.InStream, streamID)
    specOK = True
  if side in ("out", "both"):
    addOneStream(stage, stage.outStreams, streams2.OutStream, streamID)
    specOK = True
  if not specOK:
    stage.pipeLineSet.addError("Invalid side %s." % (side, ))

def select(stage, streamSpec):
  """Select a stream, e.g. 2
     aelf.output = stage.output2
     Record currently selected stream.
     Affects callpipe connectors.
     streamSpec is ordinal or streamId."""
  
## non-trivial callpipe example  
"""/* Convert userids to names */
   signal on novalue
   signal on error
   do forever
     'readto in'
     parse var in userid . +8
    'callpipe',
      ' cms namefind :userid' userid ':name ( file cottage',
      '|append literal ???',
      '|take 1',
      '|*:'
  end
  error: exit RC*(RC!=12)"""

def callpipe(stage, spec): # addpipe also
  """Create a subroutine pipeline using the spec.
     Save the parent stage in and out stream connections.
     Attach the subroutine pipeline as needed to the in and out streams of the parent stage.
     Note that the subroutine pipeline does not have a dispatcher stage. It runs as part
     of the current pipeline.
     Connector syntax:
     |--*-+----------------------------------+--:(-1)---|
          +---+--------+-+-------------------+
              +-INput--+ +---+--------+---+
              +-OUTput-+     +-*------+
                             +-stream-+"""
  subPipe = SimplePipeLine(stage.pipeLineSet, spec, sub=True)
  # depending on connectors and currently selected stream
  inx = out = 0
  ## modify our inStream and the pipelet's instream
  stage.inStreams.connect(inx, subPipe.initialStage.run1)
  
  ## modify our outStream and the pipelet's outStream
  stage.outStreams.connect(out, subPipe.stages[-1].output1)

# following is a pattern to discover the subclasses of PipeCommmand defined in this module
import sys, inspect
thisModule = sys.modules[__name__]
functions = inspect.getmembers(thisModule, inspect.isfunction)
syntaxDict = {}

def parse(cmd, syntax, func, stage, spec):
  parserFunc = syntaxDict.get(cmd)
  if parserFunc:
    return func(stage, spec)
  # create the parserlist for the command
  syntaxDict[cmd] = spec, func
  # add it to syntaxDict[cmd]
  return func(stage, spec)

if __name__ == "__main__":
  newaddStream(1,1)
