## InputFormats ##

DKPro BigData can read different kind of input data:
  * Plain Text using Text2CasInputFormat
  * Our internal representation: SequenceFiles containing CasWritables (see below)
  * Web Archives
  * Any file we have a UIMa collection reader for


## Accessing HDFS from a CollectionReader ##

since DKPro 1.5.0, all collection readers based on ResourceCollectionReaderBase support a
user-specified ResourceResolver. DKPro BigData provides a HDFSResourceLoader external Resource
that enables any DKPro collection reader to read data directly from HDFS.


## CAS Serialization Formats ##

|CASWritable | Using UIMA XMI (XML Serialization|
|:-----------|:---------------------------------|
|BinCasWritable | Using Binary Cas Serialization, very efficient |
|BinCasWithTypeSystemWritable| Prepends a Typesystem to every CAS, inefficient for small documents|


If you are sure that you will use **exactly** the same version and configuration of your components, or
as a intermediate format for shuffling, use BinCasWritable. In every other case, BinCasWithTypeSystemWritable is recommended, it it also the default.