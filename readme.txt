Downloaded from https://code.google.com/archive/p/python-pipelines/downloads on 2020-11-27.

Project appears to be dead.

================

A Python implementation of Hartmann (CMS) Pipelines.

Python Pipelines lets you solve a complex problem by breaking it up into a series of smaller, less complex programs. These simple programs, also called stages, can then be hooked together to get the results you want. The output resulting from a stage is the input to the next stage. A series of stages is called a pipeline. Each stage consists of a stage and its operands. Pipelines has many built-in stages; you may add your own written in Python or pseudo-Rexx.

Stages may have more than one input and/or output stream; these streams may be connected to other stages in no particular order.

Pipelines is a superset of pipes as found in unix/linux shells. See http://en.wikipedia.org/wiki/Hartmann_pipeline for a more detailed description.

Python Pipelines may be invoked from a command prompt or incorporated into another Python program as an imported module.

This project is under construction. Contributions are welcome. Especially Python developers, formal language parsing algorithms, tester (especially those familiar with CMS Pipelines) and potential users.

Project has been moved. Stay tuned for the url.

Please contact the project manager Bob Gailer: bgailer at gmail dot com or by phone at 1-919-636-4239.

Python Pipelines is based on and is a subset of IBM's CMS/TSO Pipelines. See the IBM Reference Link at the upper right.

In CMS Pipelines users may write their own stages using the Rexx programming language. Due to the nature of the 'dispatcher' (each stage is a co-routine) these program are more complex than they could be.

Python Pipeline supports coding the IBM way (in 'pseudo-Rexx'), which makes it somewhat easy to incorporate legacy code or write new stages using a known technology.

Ideally, however, Python Pipeline users learn to write their own stages in Python, using a much simpler approach (each stage has a run() method that is invoked as many times as needed). 7 or more lines of Rexx replaced by 2 lines of Python!