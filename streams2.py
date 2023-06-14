# streams2
import threading


class StreamEvent:  # threading.Event is a factory function, so can't subclass
    """Synchronize record arrival on instream with readto."""

    rc = 0

    def __init__(self):
        self.event = threading.Event()
        self.wait = self.event.wait

    def set(self):
        self.event.set()


class Stream(object):
    ready = True
    connection = None  # active (current) connection
    connected = False
    streamNo = None

    def __init__(self, container, stage, streamId):
        self.container = container
        self.streamNo = self.container.add(self)
        self.stage = stage
        self.streamId = streamId
        self.stageId = stage.id
        self.id = stage.id + " (%s)" % (self.streamNo,)
        self.connections = []  # stack of temporarily? disconnected connections
        self.pipeLineSet = stage.pipeLineSet
        self.event = StreamEvent()
        self.setup()

    def push(self, connection):
        self.connections.append(self.connection)
        self.connection = connection

    def pop(self, connection):
        self.connection = self.connections.pop()

    def sever(
        self, target=False
    ):  # disconnect current connection; restore stacked one if any
        if self.connections:  # any stacked connctions?
            self.connection = self.connections.pop()
        else:
            self.connected = False
            if self.side == "out":
                self.output = self.outputToSeveredStream
                # Some stages take action when discovering a stream is disconnected.
                # Example: file readers and filters terminate when they discover the
                #  primary output stream is not connected.
                # Some stages take action when discovering all streams are disconnected.
                return (
                    self.streamList.decr()
                )  # let caller know # of streams still connected


class InStream(Stream):
    """differentiated by connect method"""

    prevStage = None
    prevStream = None
    readPending = None
    type = "In"
    blocked = False

    def receive(self, packet):
        # a packet instance is transmitted from outstream to instream
        # it holds the record and eof flag, and may carry other information
        # for modifiers CASEI ZONE packet holds the original record
        # BLOCKING: stage may block input e.g., FANIN
        self.stage.inStream = self
        ## self.stage.packet = packet
        ## let output reference self.stage.packet instead of above
        if packet.eof:  # let's try this. It seems to simplify other things
            packet.eof = False  # defer sending eof to next stage
            self.eof()
        else:
            if self.blocked:
                self.event.wait()
            retval = self.run(packet.record)
            ## deal with return of stage2.PEEK?

    def setup(self, modifier=None):
        """If CASEI or ZONE precede stage:
          create appropriate modification program
          modify record
          send modified record to stage.run
          modify stream output to output original record
        If NOT precede stage:
          reverse the outstreams
        If EOFBACK precede stage:
          send record to output
          when that returns
          send record to stage"""

        # Look for runx, runany in stage.
        self.run = getattr(self.stage, "run%s" % self.streamNo, None)
        if not self.run:
            self.run = getattr(self.stage, "runany", None)
        if not self.run:
            self.run = self.defaultStageRun
        """Look for eofx, eofany in stage."""
        self.eof = getattr(self.stage, "eof%s" % self.streamNo, None)
        if not self.eof:
            self.eof = getattr(self.stage, "eofany", None)
        if not self.eof:
            self.eof = self.defaulteof

    def defaultStageRun(self, record):
        self.record = record
        if self.readPending:  # readto or peekto pending
            if self.readPending == "r":  # readto pending
                self.event.set()
                self.readPending = None
        else:
            self.readPending = "g"  # a record is available
            self.event.wait()

    def defaulteof(self):
        self.stage.eof()

    def block(self):
        self.blocked = True

    def unblock(self):
        self.blocked = False
        self.event.set()

    def connect(self, initialStage):
        """Called by CallPipe to connect stream's output
          to a run method of the new PipeLet's initialStage
        Save current stream stage."""
        self.push()
        self.stage = initialStage
        self.discover()


class NullStream:
    id = ""


class OutStream(Stream):
    """differentiated by connect method"""

    nextStage = None
    nextStream = NullStream()
    type = "Out"

    def setup(self):
        # stage writer expects there to be outputx and eofx methods
        # for each stream. Here we create those methods
        # give stage outputx and eofx methods where x is our streamNo.
        setattr(self.stage, "output%s" % (self.streamNo,), self.output)
        setattr(self.stage, "sendeof%s" % (self.streamNo,), self.eof)
        self.nextStream

    def output(self, record):
        packet = self.stage.packet
        packet.record = record
        # trace this transmission
        if self.stage.trace:
            print("%s sent '%s' to %s" % (self.id, record, self.nextStream.id))
        self.send(packet)

    def eof(self):
        packet = self.stage.packet
        packet.eof = True
        if self.stage.trace:
            print("%s sent EOF to %s" % (self.id, self.nextStream.id))
        self.send(packet)

    def send(self, *args):
        pass  # reassigned to downstream receive

    def connect(self, ref):
        """ref is presumed to be a stage output reference name.
        Save current stream stage.
        Point ref to the corresponding next stage-stream run"""
