Ecoadapters.
============

What it is 
------------

This is a collection of load/store adapter functions for Hive and Pig enabling 
data (de)serialization in/out protobuf-packed messages for use in those tools. 
What it can do: 

* read/write HBase tables and hadoop sequence files with encapsulated protobuf messages using pig
* read hadoop sequence files with protobuf messags using hive

Mainly, it enables projection of attributes, nesting of messages and mapping 
repeated protobuf fields into correspondent structures in Pig and Hive scripts. 

The motivation is very similar to elephant-bird. The distinguishing points are: 

* ecoadapters do not use code generation. Instead, one may specify either generated 
message class name or  an hdfs path to protobuf descriptor file generated 
with 'protoc --descriptor_set_out=...'. The latter is more flexible as it 
doesn't require a new code build but rather just a descriptor file update in a hdfs
to reflect protobuffer schema changes instantaneously.

(I will abstain from any feature comparisons with elephant-bird as i am not closely 
following its progress.)



Current capabilities we actively use:
-------------------------------------

#### Hive (0.5.0): 

Presenting a sequence file with protobuf messages as BytesWritable values as 
Hive external table using Hive's SerDe.

Serialization (i.e. storing back into a sequence file) is not supported. 
Usually one can reprocess this into a native hive table or run ad-hoc analytical queries.

#### Pig (0.7.0). 

* reading protobuf messages packed as values from a SequenceFile(s) with protobuf messages 
packed same way as above (BytesWritable values). 

* writing Pig output into Sequence files with protobuf messages packed the same way as above.

* reading protobuf messages from HBase columns into Pig. 

* WIP: writing pig output as protobuf messages into HBase. 


Dependencies
-------------

HBase, Hadoop and Pig dependencies are set to those in Cloudera release CDH3b3. We also 
verified these functions with standard apache releases, it's just what we currently use 
those with. 

Hive dependency is not directly available in well-known public repositories so 
i guess one would need to install Hive jars locally or remotely using maven artifact id 
 _org.apache.hive:hive-serde:0.5.0_. Or let me know of a public repository with hive 
artifacts, i'll be happy to add it.


