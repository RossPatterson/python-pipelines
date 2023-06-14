"""parsers module
Copyright 2008 under the GNU General Public License GPLv3

Generates, shelves, retrieves and calls parserCollections for stages,
   options, syntactic variables, and pipeline commands. We shelve and
   retrieve them since it takes 1/4 the time to retrieve than to generate.
  
 A parserCollection is either a list of 2 or more parsers that are applied
   consecutively to a part of a pipe spec, or a single parser.

Structure of a parser:
 Each parser has a keyword dictionary and a syntactic variable list (one of which may be empty).

Scanner's use of parsers:
 A parser is presented with a segment of a pipe spec or pipeline command spec. The first word
   of the spec is looked up in the keyword dictionary, if not empty. If it is not found and the
   syntactic variable list is empty an error is raised.
   If no keyword match and there are syntactic variable parsers, the word is applied to the
     syntactic variable parsers, one at a time until a parser returns a value.
   If no parser returns a value an exception is raised.
   If no keyword match and there are no syntactic variable parsers, an exception is raised.

Generation:
 ParserCollections are generated for subclasses of SpecificationParser that have a non-empty
   BNF attribute.
 All syntactic variables classe are saved in a shelve file sv.db.
   This must be done first, as they are referenced by the other parserCollections.
   The lower-cased class name is the key and the value is an instance of the class.
   A parserCollections is generated for those with a non-empty BNF attribute
   and assigned to the parserCollection attribute of the instance
   This is a one-time operation, normally done before distributing the program.
   
 ParserCollections for global and local options and built-in stages are generated next,
   normally before distributing the program.
   These parserCollections are saved by shelve in a file parsers.db.
 The program is smart enough to generate these db files anytime if the files are missing.
 ParserCollections for user-written stages are generated the first time the stage is
   encountered in a pipe spec. These parserCollections are also saved by shelve in parsers.db.

BNF:

  A tuple of version, BNF, return type
  version:
    When a parserCollection is generated & saved the verion # is part of the instance.
    When a parserCollection is retrieved the saved verion # is compared to the current
    version. If they do not match the parserCollection is re-generated.
    This does not apply to syntactic variables. If any of them are changed the sv.db
    must be regenerated.
  BNF:
    as described above.
  return type:
    the parserCollection returns a dict, a list or a sequence.
"""
import string # to access maketrans - to create translate table
import PipeExceptions
import shelve
import unittest
shelf = shelve.open("parsers.db") # to hold option and stage parsers
## how does this get closed?

## NOTE timing test applied to Count (10,000 reps)
## unshelving .78 sec
## creating parserList 3.3 sec (use of translate-rfind makesa little faster)

# 11/04/08 global options OK
# 11/12/08
#  opts: [SEParator xorc|LISTCMD|LISTERR|LISTRC|MSGlevel snumber]*
#  xorc: [SPACE|TABulate|char|hexpair]

version = 6

def test():
  class Test1(SpecificationParser):
    """This class is a model for objects with BNF.
       A subclass of SpecificationParser must have:
         a call to parseSpec with 
         a setup method
       and may have an BNF attribute"""
    BNF = (version, "[STReam stream|SEParator xorc|MSGlevel snumber|FOO snumber|LISTCMD|LISTERR]*", 2)
    # the pickled parserCollection for this is 639 bytes!
    name = 'Parser Test 1'
    def __init__(self, options):
      if not self.parseSpec(options):
        print("Bummer") ## really want errors
    def setup(self, specs):
      print(specs)
  #shelf.clear()
  # create an instance of Test1 and pass it a spec.
  opts = Test1("str 313 sep tab msg 3 foo 4 listrc")
  #    got  {'listrc': True, 'msglevel': 3, 'separator': ['tabulate'], 'foo': 4}
  # wanted  {'listrc': True, 'msglevel': 3, 'separator': '\t', 'foo': 4}
  opts = Test1("listrc msg 4 sep |")
  shelf.close()

class Translator:
   """Wrapper for maketrans and translate.
      Allows input of several strings with corresponding "category" """   
   input = subst = translation = ''
   def add(self, string, category):
     self.input += string
     self.subst += category*len(string)
   def finalize(self):
     self.translateTable = string.maketrans(self.input, self.subst)
   def translate(self, text):
      return text.translate(self.translateTable)
   
class SpecificationParser:
  """Some classes subclass this (stage, options, syntactic variables, pipeline commands)
     Some subclasses of these classes have BNF attributes.
     For each such class either
       Create and shelve a ParserCollection object 
       OR retrieve the shelved ParserCollection object """
  name = "Unassigned name"
  parserCollection = None
  BNF = None
  recursionDepth = 0
  """Create a translation table that gives the "categories" of all characters
     that can appear in the BNF. This gives us a "rapid lookahead" in lieu
     of testing each character. For example, given MSGlevel snumber, we
     see that M has a category of u; we then find next of category b (punctuation).
     (the blank). We then rfind next of category u, step right 1 to see if this is
     category l (lower case); if so we have an abbreviated keyword."""
  tr = Translator()
  tr.add("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "u") # upper case
  tr.add("abcdefghijklmnopqrstuvwxyz", "l") # lower case
  tr.add("0123456789", "n")                 # digit
  tr.add(" :!|[]&~()=", "b")                # separator
  tr.add("+?*", "b")                        # repeat indicator
  ## NOTE !&~() occur in ALL; () in some others
  ## parser does not yet handle these!
  tr.finalize()
  
  def parseSpec(self, specs=None):
    """Called once when subClass is first instantiated.
       If caller's class has a non-empty BNF attribute
         retrieve or create a parserCollection
         replace this method with parseWithSyntax
        else
         replace this method with parseWithoutSyntax  """
    subClass = self.__class__ # the class that is subclassing SpecificationParser e.g. Count
    if self.BNF : # attribute of the class that is subclassing SpecificationParser
      self.retrieveOrCreatePC(subClass)
    # reassign parseSpec so subsequent calls go direct
      subClass.parseSpec = self.parseWithSyntax
    else:
      subClass.parseSpec = self.parseWithOutSyntax
      
    # parserCollection setup complete; now invoke parseSpec first time
    if specs is not None:
      return self.parseSpec(specs)

  def parseWithOutSyntax(self, specs):
    """The specs are handed to setup with no preprocessing"""
    return self.setup(specs)

  def parseWithSyntax(self, specs):
    if specs:
      try:
        retval = self.parserCollection.parse(specs, self.parserCollection.mode)
      except PipeExceptions.SpecError as msg:
        return False
      values, specs = retval
    else:
      values = ''
    """Hand parsed specs to setup"""
    return self.setup(values), specs
  
  def retrieveOrCreatePC(self, subClass):
    """mode 1 - return list ['listcmd', 'msglevel', 3]
       mode 2 - return dict {'msglevel': 3, 'listcmd': True}
       mode 0 - return string 'tabulate' """      
    if len(self.BNF) == 3: # only allowable length for now
      parserCollection = shelf.get(subClass.__name__, None)
      version, BNF, mode = self.BNF
      ## not sure we need "version is None or" below
      if version is None or not parserCollection or parserCollection.version != version:
        parserCollection = self.makeParserCollection(BNF)        
        subClass.parserCollection = parserCollection
        subClass.parserCollection.mode = mode
        shelf[subClass.__name__] = parserCollection
      else:
        subClass.parserCollection = parserCollection
        subClass.parserCollection.mode = mode
    else:
      raise PipeExceptions.SyntaxError("Invalid BNF length.")

  def makeParserCollection(self, BNF):
    """Called from above and SyntacticVariable.getSvDict"""
    self.BNF = BNF.strip()
    self.category = self.tr.translate(self.BNF)
    self.index = 0
    try:
      parserCollection = self.makeParsers() # recursive part
    except PipeExceptions.SyntaxError as errorMsg:
      # first error terminates parsing a BNF
      # we need to pass errorMsg on so the scanner can report it.
      self.error = True
      return False
    parserCollection.version = version
    return parserCollection
    
  def makeParsers(self):
    """recursively called"""
    self.recursionDepth += 1
    parserCollection = ParserCollection() # creating ONE of these
    while self.index < len(self.BNF):
      parser = self.setupParser()
      parserCollection.append(parser)
      if self.recursionDepth:
        # for reasons I don't remember there can be an extra empty parser at end of the collection.
        if not parserCollection[-1]:
          raise "EXTRA PARSER IN COLLECT" # del parserCollection[-1]
        break

##    if len(parserCollection) == 1:
##      # parserCollection becomes the lone parser
##      parserCollection = parserCollection[0]
##      parserCollection.mode = parserCollection.mode
##      parserCollection.version = version
      
    self.recursionDepth -= 1
    return parserCollection

  def setupParser(self):
    parser = Parser()
    parser.kwdDict = {}
    parser.svParsers = []
    parser.name = "group%s" % Parser.parserCount # override with name?
    inGroup = False
    start = self.index
    end = len(self.BNF)
    token = None
    # walk the BNF until we are complete or find an error
    try:
      while self.index < end:
        ch = self.BNF[self.index]
        cat = self.category[self.index]
        if cat in 'uln': # alphanumeric - start of token
          self.startOfToken = self.index
          self.index = self.category.find('b', self.index)
          self.token = Token(self)
        else: # non-alphanumerics handled differently in or not in a group
          if inGroup:
            if ch == '[': # recursive call
              if self.token:
                parserCollection = self.makeParsers()
                self.processToken(parserCollection)
                return parser
              else:
                raise PipeExceptions.SyntaxError("token missing")
            elif ch == ':': ## ensure this is first in group?
              if self.name:
                raise PipeExceptions.SyntaxError("duplicate parser name")
              else:
                parser.name =  self.token
            elif ch == '!': ## ensure this is 1st or 2nd in group?
              if parser.default:
                raise PipeExceptions.SyntaxError("duplicate default")
              else:
                ## this section is not complete - pList is never defined!
                parser.default = self.token
                parser.required = False
                self.processToken(pList)
            elif ch == '|':
              if self.token:
                self.processToken()
              else: ## ONLY valid when first |
                self.parser.required = False              
            elif ch == ' ': # blank in group: create (sub)parserCollection
              if self.token:
                self.index += 1
                parserCollection = self.makeParsers()
                self.processToken(parserCollection)
                self.index -= 1 # backup to reprocess last ]
            elif ch == ']': # end of group - get repetition & return
              if self.token:
                self.processToken()
              self.index += 1
              inGroup = False
              candidateIndex = self.index
              if candidateIndex < len(self.BNF) and self.category[candidateIndex] == 'r':
                self.parser.repeat = self.BNF[candidateIndex] # * 0 or more, + 1 or more, ? 0 or 1
                if self.parser.default and self.parser.repeat == "+":
                  raise PipeExceptions.SyntaxError("default value conflicts with + repeat count")
                candidateIndex += 1
                if candidateIndex < len(self.BNF) and self.BNF[candidateIndex] not in " |]":
                  startOfToken = candidateIndex
                  candidateIndex = self.category.find('b', candidateIndex)
                  if candidateIndex >= 0:
                    self.index = candidateIndex
                    self.parser.repeatSep = self.BNF[startOfToken:candidateIndex]
                  else:
                    self.parser.repeatSep = self.BNF[startOfToken:]
                self.index = len(self.BNF)
              else:
                return parser
          else: # not inGroup
            if ch == '[':
              inGroup = True
            elif ch == ' ':
              self.processToken()
#              return
            elif ch in '|]':
              if self.recursionDepth > 1:
                if self.token:
                  self.processToken()
                return parser
              else:
                raise PipeExceptions.SyntaxError("unexpected character '%s' outside group" % ch)
            else:
              raise PipeExceptions.SyntaxError("unexpected character '%s' outside group" % ch)
          self.index += 1
    finally:
      parser.syntaxSpec = self.BNF[start:self.index]
      return parser

  def processToken(self, value=None):
    """Called when parser discovers the token's value.
       Depending on kind of token:
        add to keyword dictionary, expanding abbreviations
        add to syntactic variable parsers list
    """        
    initialCharCategory = self.token.tokenCategory[0]
    if initialCharCategory == "u": # keyWord starts with cap(s); may end with lower case
      ## I am not sure that this is the correct thing to do ...
      key = self.token.lower()
      if value:
        item = Keyword(key, value)
      else:
        item = BooleanKeyword(key)
      self.expandDictionaryEntry((key, item))
    elif initialCharCategory == "l": # leading lower case
      # syntactic variable
      parser = svDict.get(self.token.token, None)
      if parser:
        self.parser.svParsers.append(parser)
      else:
        raise PipeExceptions.SyntaxError("Unexpected syntactic variable %s" % (self.token, ))
    elif initialCharCategory == "n": # leading digit - (if not default) see apldecode et.al.
      ## WHAT ABOUT leading digit as in APLDECODE 3279, BLOCK .. 15 3F?
      if not self.default or index:
        self.parser.kwdDict[abbrev] = word
    self.token = None

  def expandDictionaryEntry(self, keyVal):
    """create dictionary entries for all abbreviations of the keyword"""
    lastUpper = self.tokenCategory.rfind('u') + 1 # discover rightmost upper case   
    abbrev = self.token[:lastUpper].lower()
    self.parser.kwdDict[abbrev] = keyVal
    for ch in self.token[lastUpper:]: # any lower case following?
      abbrev += ch
      self.parser.kwdDict[abbrev] = keyVal
  
class Token:
  """instantiated when parser discovers a token."""
  def __init__(self, sp): # specParser
    self.token = sp.BNF[sp.startOfToken:sp.index]
    if self.token.isalnum():
      self.tokenCategory = sp.category[sp.startOfToken:sp.index]
## following anticipates BNF extension to provide a value for a keyword
## example - in xorc SPACE gets a value of ' ', TABULATE gets '\t'
##  follower = ''
##  expression = ''
##      if specParser.index < len(specParser.BNF) - 1:
##        self.follower = specParser.BNF[specParser.index]
##        if self.follower == '=': # expression follows
##          x = specParser.BNF[specParser.index+1:].find('b')
##          self.expression = specParser.BNF[specParser.index+2:x]
##          self.index = x
##      parser.expression = self.expression
    else:
      raise PipeExceptions.SyntaxError("unexpected character following '%s'" % token)
      
class ParserCollection(list):
  """collection of an object's parsers"""
  mode = 1 # can be changed during initialization by a BNF value

  def parse(self, specs, mode):
    ## WARNING - this does NOT collect the results just returns the last parser's results
    for parser in self:
      if specs:
        values, specs = parser.parse(specs, mode) # seq of values of consumed tokens, leftover specs
      else:
        if parser.required:
          raise PipeExceptions.SpecError("Missing value for %s" % (self.syntaxSpec, ))
        elif parser.default is not None:
          values = [parser.default]
    return values, specs

  def x__repr__(self):  
    return '\n'.join(parser.__repr__() for parser in self)

class Parser:
  """objects with BNF have a sequence of parser instances, one per group.
     Some parser instances may have a (nested) sequence of parser instances.
     For example (subset of global options) LISTRC|MSGlevel snumber
     There is 1 top-level parser with a keyWord dictionary holding LISTRC and MSGlevel.
     MSGlevel's entry points to another parserList for snumber.
     The 2nd parser has its own parserlist for snumber
     """ 
  name = None 
  default = None
  repeatSep = '' 
  repeat = '' # +?*
  required = True
  token = None
  parserCount = 0
  
  def __init__(self):
    Parser.parserCount += 1
      
  def __bool__(self):
    return bool(self.kwdDict or self.svParsers)
      
  def parse(self, specs, mode): 
    """Called by ParserCollection.parse or directly in lieu of ParserCollection.parse
       Ways to return results:
        Seq  - ['listcmd', 'msglevel', 3] - ideal when order is sufficient
        Dict - {'listcmd': True, 'msglevel': 3]} - ideal for options!
        Str  - for svParser"""
    values = Parser.valuesContainer[mode]()
    while specs:
      words = specs.split(None, 1)
      rest = "" if len(words) == 1 else words[1]
      word = words[0]
      item = self.kwdDict.get(word, self.svParsers) # keyword?
      # item is an instance of an Item subclass with a parse method called to get desired behavior
      specs = item.parse(specs, values)

      """ old code now folded into the 3 item classes      
            if item is None: # syntactic variable?
              for svParser in self.svParsers:
                item, specs = svParser.parse(specs)
                if item is not None:
                  values.add(item)
                  break
              else:
                raise PipeExceptions.SpecError, "unmatched word(s) '%s'" % specs
            elif isinstance(item[1], list): # assume this is a parserCollection 
              words = specs.split(None, 1)
              specs = '' if len(words) == 1 else words[1]
              valuesList, specs = item[1].parse(specs, item[1].mode)
              values.add(item[0], *valuesList)
            else: # kwd w/o parserCollection
              values.add(item[0])
              specs = rest
      """        
      if not self.repeat: ## problem with kwd and
        ## properly handle various repeat types!
        # * 0 or more, + 1 or more, ? 0 or 1
        break
    if values or not self.required:
      return values.get(), specs
    raise PipeExceptions.SpecError("required word missing")

  def x__repr__(self):
    repr = ''
    for item in list(self.kwdDict.items()):
      repr += item[0]
      if isinstance(item[1], list):
        repr += ': ' + item[1].__repr__() + '\n'
      else:
        repr += '\n' 
      for item in self.svParsers:
        repr += item
    return repr

  class Seq(list):
    def add(self, *values):
      self.extend(list(values))
    def get(self):
      return self
      
  class Str(Seq):
    def get(self):
      return ' '.join(self)

  class Dict(dict):
    def add(self, key, value=True):
      self[key] = value
    def get(self):
      return self

  valuesContainer = (Str, Seq, Dict)

class Item:
  """Superclass of the 3 item classes.
     Items are the value half of the keyword dictionary."""

class Keyword(Item):
  """A keyword dict value that has a parserCollection.
  """
  def __init__(self, kwd, parserCollection):
    self.kwd = kwd
    self.parserCollection = parserCollection

  def parse(self, specs, values):
    words = specs.split(None, 1)
    specs = '' if len(words) == 1 else words[1]
    valuesList, specs = self.parserCollection.parse(specs, item[1].mode)
    values.add(self.kwd, *valuesList)
    return specs
        
class BooleanKeyword(Item):
  """A keyword dict value whose presence indicates True.
  """
  def __init__(self, kwd):
    self.kwd = kwd

  def parse(self, specs, values):
    values.add(self.kwd)
    specs = rest
    return specs

class TestParser(unittest.TestCase):
  def setUp(self):
    shelf = shelve.open('test')
  def testParser(self):
    shelf.clear()
    opts = Test1("listcmd msg 3")
    print(opts.specs)
    self.assertEqual(opts.specs, "listcmd msglevel 3")
    self.assertTrue(shelf.haskey("Test1"))
    self.assertTrue(Test1.new)
    opts = Test1("listcmd msg 3")
    self.assertTrue(not Test1.new)
      
def oldMain():
  BNF = "[KWA|KWB number [KWD|xorc]|KWC]*?"
  parser = ParserCollectionFactory(BNF)
  values = parse(parser.parserList, "kwc kwa")
  a = 3
#  unittest.main()
  

# ------------------ Syntactic Variables ---------------------
# 12/06/08 - new idea: save svparsers in separate shelf
# which generate & distribute.

# goal is to "bridge the gap" between top-level references to svparsers
# and the actual parser without pickling the sv parser in the top level
# ENDchar xorc -> {'end': ('endchar', instance of syntacticVariables.XorC)}

# initially create a writeback shelf (svDict) {'xorc', instance of XorC, ...}
# from the class definitions,
# for each sv with BNF
#   create parserCollection
#   assign it to sv parserCollection property
  
      
class SyntacticVariable(object, SpecificationParser):
  """Superclass of syntactic variable classes (p 194 ff).
     Takes entire spec as input, removes what it needs and returns the rest.
     Applies to all sv's that can include embedded blanks.
     
     TokenSyntacticVariable subclass applpies to those taking one word.

     When parsing BNF (e.g. endchar xorc) in processToken
     we may find an sv. For that we must return the parserCollection.
     
     When parsing specs we give the spec to the sv parser which
     tries to match the start or all of the spec.
     """

  def getSvDict(self):
    global svDict
    import os.path
    fn = 'sv.db'
    if os.path.exists(fn):
      svDict = shelve.open('sv.db', flag = 'r') # {classname(lower): instance of sv class, ...}
    else: # must create - one time activity
      from inspect import isclass
      svdict = dict((name.lower(), cls()) for name, cls in list(globals().items())
                    if isclass(cls) and issubclass(cls, SyntacticVariable))
      svDict = shelve.open('sv.db', flag='n', writeback=True)
      svDict.update(svdict)
      for key, svInst in list(svdict.items()):
        if svInst.BNF:
          version = mode = 0
          BNF = svInst.BNF
          pc = self.makeParserCollection(BNF)
          svInst.parserCollection = pc
      svDict = shelve.open('sv.db', flag = 'r') # save changes and open normally

  def parseSpec(self, specs=None):
    if self.BNF : # attribute of the class that is subclassing SpecificationParser
    # reassign parseSpec so subsequent calls go direct
      self.parseSpec = self.parseWithSyntax
    else:
      self.parseSpec = self.parseWithOutSyntax
    return self.parseSpec(specs)

  def parseWithOutSyntax(self, specs):
    return self.subParse(specs), ''

  def parseWithSyntax(self, specs):
    if specs:
      try:
        retval = self.parserCollection.parse(specs, self.parserCollection.mode)
      except PipeExceptions.SpecError as msg:
        return False
      values, specs = retval
    else:
      values = ''
    return self.subParse(values), specs

  def parse(self, spec):
    value = self.subParse(spec)
    rest = spec[len(value):]
    return value, rest
  
  def setup(self, values):
    return self, values
  
class TokenSyntacticVariable(SyntacticVariable):
  """syntactic variable classes that take 1 blank delimited token"""
  def parse(self, spec):
    specs = spec.split(None, 1)
    rest = "" if len(specs) == 1 else specs[1]
    word = specs[0]
    value = self.parseSpec(word)[0]
    if value:
      return value, rest
    else:
      return "", spec
    
class Xorc(TokenSyntacticVariable):
  ## xorc has a BNF diagram hence uses a parser
  """>>--+--SPACE----------+--><
         +--TABulate-------+
         +--char-----------+
         +--hexpair--------+
     A character specified as itself (a one-character word) or its
     hexadecimal representation (a two-character word). The blank is
     represented by the keyword BLANK, which has the synonym SPACE, or
     with its hex value, X'40'. The default horizontal tabulate
     character (X'05') is represented by the keyword TABULATE, which
     can be abbreviated down to TAB."""
  BNF = '[SPACE|TABulate|char|hexpair]'
  # this is in a parserCollection with one parser
  # the final act of the SpecificationParser is to call setup with the
  # parsed token.
  ## FO add expression to the BNF?
  ##  [SPACE=' '|TABulate='\t'|char|hexpair=int(x,16)]
  def subParse(self, token):
    token = token[0]
    if token == "space":
      value = " "
    elif token == "tabulate":
      value = '\t'
    elif 1 <= len(token) <= 2:
      value = token
    else:
      raise PipeExceptions.SyntaxError("Invalid xorc token '%s'." % token)
    return value

class Char(SyntacticVariable):
  """Any 1 char except BLANK."""
  def subParse(self, spec):
    if spec[0] != ' ':
      return spec[0]

class HexPair(TokenSyntacticVariable):
  """NEW 2 hex digits. Initially invented for xorc."""
  def subParse(self, spec):
    if len(spec) == 2:
      try:
        value = chr(int(spec, 16))
        return value
      except:
        raise PipeExceptions.SyntaxError("Invalid hexChar '%s'" % spec)
    else:
      raise PipeExceptions.SyntaxError("Invalid hexChar '%s'" % spec)

class DecimalNumber(TokenSyntacticVariable):
  """NEW decimal number - sequence of decimal digits with optional period
     Initially invented for BEAT stage."""
  def subParse(self, spec):
    left, right = spec.split2('.')
    if left and right:
      try:
        lvalue = int(left)
        rvalue = int(right)*0.1**len(right)
        return lvalue + rvalue
      except:
        pass

class DelimitedString(SyntacticVariable):
  """See p 294 or delimitedstring.py for the details.
     One of:
      xXhH followed by 2 hex chars 0-F 0-f
      bB followed by 0's and/or 1's in multiples of 8
      Optional STRING Delim+String+Delim e.g. /cat/"""
  def subParse(self, spec):
    value = ''
    token, spec = spec.split2() # [string hFF b11110000 /cat/, ???]
    if token.lower() == 'string': 
      if not spec: # nothing follows! bad!
        error("'string' can't be last token")
    else: # without leading 'string' could be hex or binary
      c = token[0]
      if c in 'hHxX':
        if len(token) == 3:
          try:
            value = chr(int(token[1:]), 16)
          except:
            error("invalid hex token")
        else:
          error("invalid hex token")
      elif c in 'bB':
        if len(token) % 8 ==1:
          try:
            value = ''.join(chr(int(token[b:b+8],2)) for b in range(0,len(token),8))
          except:
            error("invalid binary token")
        else:
          error("invalid binary token")
    if not value:
      endDelimIndex = specs.find(s[0],1)
      if endDelimIndex:
        value = specs[1:endDelimIndex]
        rest = specs[endDelimIndex:]
      else:
        error('asdf')
    return value

class InputRange(SyntacticVariable):
  def subParse(self, spec):
    """ """
    ## for now consume nothing return blank
    return

class InputRanges(SyntacticVariable):
  def subParse(self, spec):
    """ """
    ## for now consume nothing return blank
    return

class Number(TokenSyntacticVariable):
  def subParse(self, token):
    try:
      value = int(token)
      if value >= 0:
        return value
    except:
      return

class NumberOrStar(TokenSyntacticVariable):
  BNF = '[number|star]'
  def subParse(self, token):
    """+----number----
       +-*------+"""
    return token

class QuotedString(SyntacticVariable):
  def subParse(self, spec):
    """A quoted string is written in the REXX fashion. Two consecutive
       quotes within the string are replaced with a single one"""
  
class Range(SyntacticVariable):
  def subParse(self, spec):
    """a single number, the beginning and end of the range with a hyphen
       ('-') between them, or the beginning number and the count with a
       period ('.') between them.
       The first number in a range must be positive. The last number in a
       range specified with a hyphen must be larger than or equal to the first
       one. An asterisk in the first position is equivalent to the number 1. An
       asterisk after a hyphen specifies infinity, the end of the record, or all
       records in a file."""

class Snumber(TokenSyntacticVariable):
  def subParse(self, token):
    """A signed number can be positive, zero, or negative. Negative numbers
       have a leading hyphen; zero and positive numbers have no sign. The 
       smallest number supported is -2**31 (-2147483648)."""
    try:
      value = int(token)
    except:
      value = None
    return value

class String(SyntacticVariable):
  def subParse(self, spec):
    """A string is a sequence of characters with or without blanks. It can
       have leading and trailing blanks; a string extends to the stage
       separator."""
    return spec

class Stream(TokenSyntacticVariable):
  """--+-number---+---
       +-streamID-+"""
  BNF = '[number|streamid]'
  def subParse(self, token):
    return token[0]
  
class StreamID(TokenSyntacticVariable):
  """A stream identifier is a word having up to four characters. It cannot be
     a number. Case is respected in stream identifiers."""
  def subParse(self, spec):
    if spec.isdigit():
      raise PipeExceptions.SpecError("streamid '%s' must not be a number" % (spec, ))
    return spec

class Word(TokenSyntacticVariable):
  def subParse(self, token):
    """ """
    return token

class Star(TokenSyntacticVariable):
  def subParse(self, token):
    """ accept an asterisk """
  def subParse(self, token):
    if spec == '*':
      return token

'''
class X(SyntacticVariable):
  def subParse(self, spec):
    
    """ """
class Y(TokenSyntacticVariable):
  def subParse(self, token):
    """ """
'''

## FO add xrange hexString fn ft fmode fm dirid 198
sv = SyntacticVariable()
sv.getSvDict()

def test2():
  kwDict = {'end': ('endchar', Xorc)}
  import pickle
  print(pickle.dumps(kwDict))

if __name__ == "__main__":
  test()

"""These notes are not 100% up-to-date

Following is about as complex as it can be (except SPEC, ZONE, ?? of course)
                                     v--------------------------< 
>>--<SFS--fn--ft--dirid---+-------+--+------------------------+-^---><
                          +-digit-+  +-ASIS-------------------+ 
                                     +-ESM--delimitedString---+ 
                                     +-OLDDATERef-------------+ 
                                     +-WORKUNIT--+-number--+--+ 
                                                 +-DEFAULT-+ 
                                                 +-PRIVATE-+

Human translates that diagram into a BNF specification.
FO write a program to do the translation, or to generate the parsers from
the diagram?

Result:

BNF = (1, # version id
  "filename: fn ft dirid "  # key: space-separated items
  "filemode: digit?"        # key:, item 0-or-1  
  "[|ASIS|ESM delimitedString|OLDDATERef|WORKUNIT [number|DEFAULT|PRIVATE]]*")
  # no key [group-of-|separated-items)multiple-choice

+ = 1 or more
? = 0 or 1
* = 0 or more
Any of the above can be followed by a word (no intervening blank).
That word is the separator between multiple choices. Default is BLANK

[] is required around anything to be considered a group that has embedded blank
word: is a name applied to what follows (group or sequence)
within a group that can have 0 matches word! if present is the default

The object (stage, ..) gets a list of parsers. Each parser has:
 - a list of syntactic variable objects (possibly empty).
 - a keyword-lookup dictionary (possibly empty).
    dictionary keys are:
      full-keyword - the value is
        if a group follows the keyword, a list of parsers (recursive definition)
        else the full-keyword 
      abbreviation of a keyword
        if the corresponding full-keyword value is a list of parsers
          the abbreviation value is *full-keyword
        else
          the abbreviation value is full-keyword

Parsing a stage spec yields a list of items, in left-to-right order.
Item is (key,value,value,..).
For example <SFS cms exec a asis esm /foo/
becomes [("filename", "cms", "exec", "a"), ("asis",), ("esm", "/foo/")]
This list is passed to the stage's setup method which configures the stage.

The parsers for <SFS are:
  [fn-parser, ft-parser, dirid-parser], [mode-parser], [kwd-parser]
  1st 4 are only syntacticvariable parsers
  last is only a keyword parser (dictionary):
  {'asis':'asis',
   'esm': [delimitedString-parser],
   'olddater': 'olddateref', 'olddatere': 'olddateref', 'olddaterf': 'olddateref', 
   'workunit': [number|default|private parser]}
ESM has a list of 1 parser: [delimitedString-parser]
WORKUNIT has a list of 1 parser: [number|default|private parser]. Here there is a
syntactic variable object and a keyword object.

Given (from Pipe Reference or custom stage design):
  stage definition = BNF diagram
  BNF diagram = group [group ..]

  we define word as either:
   keyword (e.g. ASIS, 3278) begins with upper case or digit.
   syntactic variable (fn, ft, dirid) all lower case (page 194 ff)

  group = (recursive definition)
   optional multiple selection indicator <--------< as 1st line
   NOTE in at least one case a character appears in that line <---&---<
   optional default word 1st line (above main line)
  note recursion will translate to nested sequence

Human action:
  for each group sequenct
    define a keyname (may be optional)
    translate group to reg-exp
  combine keyname and reg-exp into tuple
    spec -> (keyname, reg-exp)
  combine version with tuples into BNF
    BNF = (version, spec0, [spec1, ..])

Pipeline scanner - first call to setupMaster for a stage Class runs parserFactory
  BNF -> (nested) sequence of parser objects
    if shelved retrieve
    else generate and shelve

Pipeline scanner - each call to setupMaster

  Stage BNF presented here so we can use the term "string"
     Stage: >-+---------+-+------------+-+-word-+--------+-<
              +-label-+ +-localOptions-+        +-string-+

  wordList = string.split()
  apply parsers to wordList
  errors:
    required item not found
    wordList not empty or empty too soon

Parser application
  input = wordList
  match word from wordList (repeat if multiple selection)
    lookup in dictionary
    try for a syntactic variable
  if found
    pop word from wordList
    accumulate values
    loop back if repeat
  return values or error
  errors:
    required item not found
"""
