import importlib

try:
    importlib.reload(parsers)
except:
    import parsers

try:
    importlib.reload(streams2)
except:
    import streams2

try:
    importlib.reload(pipeCommands)
except:
    import pipeCommands


class Packet:
    """Holds the data record and other info passed from stage to stage"""

    eof = False
    record = None

    def __init__(self, record=None):
        self.record = record

    def __repr__(self):
        if self.eof:
            return "At EOF"
        else:
            return self.record


class Streams(list):
    """base class for InStreams and OutStreams"""

    count = 0

    def add(self, stream):
        self.append(stream)
        self.count += 1
        return self.count  # streamNo


class InStreams(Streams):
    """Each stage has an instance for all its inStreams."""


class OutStreams(Streams):
    """Each stage has an instance for all its outStreams."""


class InvertibleStage:
    """Mixin for stages that can be preceded by N e.g. locate nlocate.
    Either use this or invertible property."""


class Stage(parsers.SpecificationParser):
    """Base class for all stages."""

    parsers = None
    streamSpecs = "!!"  # default primary only, in & out required
    streamCount = 0
    maxStreams = 99  ## FO replace with parsing of streamSpecs
    trace = 0  # can be set from global / local options
    status = "active"
    rc = 0
    invertible = False
    first = True
    currentOut = None
    currentIn = None
    BNF = None  # some stages override this
    modifier = False
    streamsByID = {}  # relate streamID to its stream

    def __init__(self, name, pipeLineSet, simplePipeLine, cls, stageSpec):
        if Stage.first:
            # add an instance of each pipeline command to the Stage class
            ## FO explore initially setting these as stubs that replace themselves
            ## with the real thing at first call. Real thing is a method that invokes
            ## a method in the command class
            Stage.first = False
            for func in pipeCommands.functions:
                setattr(Stage, func[0], func[1])
        self.name = name
        self.inStreams = InStreams()
        self.outStreams = OutStreams()
        self.packet = Packet()  # for source stages
        self.pipeLineSet = pipeLineSet
        self.simplePipeLine = simplePipeLine
        self.parseSpec(stageSpec, self)

    def finalizeMaster(self):
        """Called by scanner after pipe assembled to check streams vs streamSpec
        and do any other stage-specific finalization."""
        self.finalize()

    def finalize(self):
        """Called by scanner after all stages created.
        Some stages override for specific tasks."""

    def log(self, msg):
        print("%s %s" % (self.id, msg))

    def initialize(self):
        """Called each time pipe is run.
        Some stages override e.g. file i/o to open; count to reset totals."""

    #  def setupMaster(self, *args):
    #    self.setup(*args)
    def setup(self, *args):
        """Called by scanner after stage is instantiated so stage can process
        its args."""

    def eof(self):
        """Default action when an instream receives an eof packet."""
        self.sendeof1()  # pass eof to primary output
        self.exit()  # terminate stage

    def output(self, record, streamNo=None):
        """output to specified or selected stream"""
        if stream == None:
            streamNo = self.currentOut
        getattr(self, "output%s" % streamNo)(record)

    # following needed for stages that could be sinks (e.g. Console1)
    def output1(self, *arg):
        pass

    def sendeof1(self):
        pass

    # or have optional secondary outstream (e.g. locate)
    def output2(self, *arg):
        pass

    def sendeof2(self):
        pass

    def exit(self, RC=0):
        self.status = "terminated"
        self.rc = RC

    def short():
        self.inStreams[0].run = self.outStreams[0].send

    # pipeline commands

    def addStream(self, side="both", streamID=None):
        """Implements the pipeline command:
                       +-BOTH---+
        >>--ADDSTREAm--+--------+-+-----------+-><
                       +-INput--+ +-streamID-+
                       +-OUTput-+
           Used also by stageFactory when creating a stage
           and when handling a label reference."""
        ## FO connect pipeline commands to BNF parser?
        self.streamCount += 1
        side = side.lower()
        specOK = False
        if side in ("in", "both"):
            self.addOneStream(self.inStreams, streams2.InStream, streamID)
            specOK = True
        if side in ("out", "both"):
            self.addOneStream(self.outStreams, streams2.OutStream, streamID)
            specOK = True
        if not specOK:
            self.pipeLineSet.addError("Invalid side %s." % (side,))

    def addOneStream(self, container, cls, streamID):
        if len(container) == self.maxStreams:
            self.pipeLineSet.addError("Too many streams.")
        stream = cls(container, self, streamID)
        container.append(stream)

    def select(self, streamSpec):
        """Select a stream, e.g. 2
        aelf.output = self.output2
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

    def callpipe(self, spec):  # addpipe also
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
        subPipe = SimplePipeLine(self.pipeLineSet, spec, sub=True)
        # depending on connectors and currently selected stream
        inx = out = 0
        ## modify our inStream and the pipelet's instream
        self.inStreams.connect(inx, subPipe.initialStage.run1)

        ## modify our outStream and the pipelet's outStream
        self.outStreams.connect(out, subPipe.stages[-1].output1)


class ModifierStage(Stage):
    """CASEI NOT ZONE"""

    modifier = True


class NullStage(Stage):
    """stage factory returns instance when error in spec."""

    def __init__(self):
        pass


if __name__ == "__main__":
    # test importing & calling pipe command functions
    x = Stage("Stage x", 0, 0)
    assert hasattr(x, "addStream")
    x.newaddStream(21)  # first call should save things in pipeCommands.syntaxDict
    x.newaddStream(31)  # subsequent call retrieves them
