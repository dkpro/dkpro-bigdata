# Introduction #

DKPro BigData is a collection of components for text analytics using uima
on Apache Hadoop.


# Input/Output #

DKPro BigData facilitates data exchange between Hadoop and UIMA in various
ways.
It provides components to read collections using UIMA for processing using
Map/Reduce and vice versa.

Moreover it contains readers for some common WebArchive formats, which enable e.g. linguistic processing of CommonCrawl data.


## Terminology ##
  * A CAS is the UIMA datastructure encapsulating a document and it's annotations.
  * an UIMA Analysis Engine process CASes by analyzing the contents and adding annotatins.
  * A UIMA CollectionReader is a component for reading a collection of components and representing them as CAS
  * Hadoop Writables implement a Hadoop API for serialiazation

[more information about IO](InputOutput.md)

# Execution of  DKPro Pipelines on Hadoop #


# Collocation extraction from UIMA annotated corpora #