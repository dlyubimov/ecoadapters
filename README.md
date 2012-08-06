Ecoadapters
============

What it is 
------------

This is a collection of load/store adapter functions for Hive and Pig enabling 
data (de)serialization in/out protobuf-packed messages for use in those tools. 
What it can do: 

* read/write HBase tables and hadoop sequence files with encapsulated protobuf messages using pig
* inline protobuf message parsing in Pig (obviously, very useful as lazy parsing technique 
when optimizing for performance)
* read hadoop sequence files with protobuf messags using hive
* inline reading of an hbase value as bytes or protobuf message from hbase (HBaseGet)
* protobuf mapping to a (lazy or non-lazy) R lists. Running R closures in a MapReduce tasks 
  (similar to R-Hadoop) but specifically adapting to sequence files with encapsulated 
  protobuf messages.
* misc 

Mainly, it enables projection of attributes, nesting of messages and mapping 
repeated protobuf fields into correspondent structures in Pig and Hive scripts. 

The motivation is very similar to elephant-bird. The main features are: 

* ecoadapters do not use code generation. Instead, one may specify either generated 
message class name or  an hdfs path to protobuf descriptor file generated 
with 'protoc --descriptor_set_out=...'. The latter is more flexible as it 
doesn't require a new code build but rather just a descriptor file update in a hdfs
to reflect protobuffer schema changes instantaneously.

(I will abstain from any feature comparisons with elephant-bird as i am not closely 
following its progress.)



Current capabilities we actively use:
-------------------------------------

#### Hive (0.7.0): 

Presenting a sequence file with protobuf messages as BytesWritable values as 
Hive external table using Hive's SerDe.

Serialization (i.e. storing back into a sequence file) is not supported. 
Usually one can reprocess this into a native hive table or run ad-hoc analytical queries.

#### Pig (0.7.0, 0.8.0). 

* reading protobuf messages packed as values from a SequenceFile(s) with protobuf messages 
packed same way as above (BytesWritable values). 
* writing Pig output into Sequence files with protobuf messages packed the same way as above.
* reading protobuf messages from HBase columns into Pig. 
* writing pig output as protobuf messages into HBase. (as of tag 'HBaseProtobufStorage-verified '). 
* dev-0.x branch is compiled with pig 0.8.0 and tested. Release 0.2 is the last one compiled with pig 0.7.0 
but I used compiled 0.2 jar with pig 0.8.0 too so compiled artifact should stay compatible with pig 0.8.0 and 
0.7.0 until we hit some incompabitibilty between 0.7.0 and 0.8.0 trees at which point one looking for 0.7.0 compatible 
artifact should probably should go back to ecoadapters-0.2. (releases are marked with tags). 

#### example: reading protobuf messages from sequence files into pig script: 

To give an idea what this stuff can do, here's an actually ran  
example reading protobufs from a sequence file into pig:

     register protobuf-java-2.3.0.jar;
     
     A = load '/data/inadco/var/log/IMPRESSION/*'
     using com.inadco.ecoadapters.pig.SequenceFileProtobufLoader(
     'com.inadco.logging.codegen.test.TestMessages$TestLogProto');
     
      -- or alternatively:
     
     A = load '/data/inadco/var/log/IMPRESSION/*'
     using com.inadco.ecoadapters.pig.SequenceFileProtobufLoader(
     'hdfs://localhost:11010/data/inadco/protolib/testMessages.protodesc?msg=inadco.test.TestLogProto');
     
      -- and then test it
      
     describe A;
     A: {LandingPageTitle: chararray,LandingPageKeyword: chararray,UniqueURL: chararray,IsDelete: boolean,IsNew: boolean,IsDirty: boolean,___ERROR___: chararray}
 
Many functions, along as SequenceFileProtobufLoader example above,
accept `proto-spec` parameter which can be defined as 

    proto-spec = hdfs-proto-spec | class-proto-spec 
    hdfs-proto-spec = protodec-hdfs-url '?' 'msg' '=' fully-qualified-message-name
    class-proto-spec = msg-class-name

(as subject to RFC-822). 

That is, you can specify message descriptors by specifying either generate message 
class name (but that requires the hassle of making sure pig backend jobs see it 
in their class path) or you can just specify it by protobuf descriptor file 
found anywhere on the hdfs the job is running on (more dynamic way of integrating 
protobuf schema into your app).

#### Pig Eval funcs

I recently added pig eval functions `Pig2Proto()` and `Proto2Pig()` which do inline conversion
with syntax similar to store and load funcs . 
 
This is still under development, I'll add more status to it as it gets verified.  
Proto2Pig function is expected largely to work in all cases pretty seamlessly. 

The case of Pig2Proto is not coming together as nicely. The main problem is that 
I need to send incoming pig schema to all instances of backend invocation. While 
it is easy to do so for a function instance, it is not going to work as nicely 
in case of multiple invocation. We can use pig's `define` functionality to separate 
one invocation from antoher, but this technically relies on a convention that we 
don't invoke same `define`'d function multiple times on perhaps different input 
schemas. I feel that relying on user-forced conventions is a bad choice, so 
Pig2Proto function is now in limbo while I am considering workarounds (or perhaps 
until Pig starts supporting invocation instance configuration more smoothly than 
it does it right now).


#### HBaseGet Eval func

I added new function, *HBaseGet*. Giving RDBMS execution plan analogy, sometimes 
it is more practical to implement joins and merges using 'nested loops' optimization 
instead of 'sorted merge' execution plan. Given that analogy, HBaseProtobufLoader 
corresponds to plans with 'sorted merge' or 'full scan' and HBaseGet would correspond 
to joins that use nested loops. Obivously, nested loops may provide a big win if the 
amount of joined records is far less than the entire table (which is quite 
a case i came recently across  while implementing certain incremental 
algorithm).

*HBaseGet*, same as Proto2Pig, uses `define` to receive table name and column specification 
string. Column specification string is the same format as HBaseProtobufLoader would accept
(standard pig HBaseLoader uses 

    column-spec = 1*(family ':' qualifier) 

grammar and I just extened this by 

    column-spec = 1*(family ':' qualifier ':' proto-spec) 

where proto-spec grammar is given earlier).

   

Dependencies
-------------

#### HBase, Hadoop, Pig

HBase, Hadoop, Hive and Pig dependencies are now set to those in Cloudera release CDH3u0 at 
the 0.3.2-SNAPSHOT. 0.3.1 was released and tagged and contained CDH3b4 dependencies. 

Current HEAD is at CDH3u3.

R stuff was developed and tested  on R 2.13 and 2.14 

We also verified these functions with standard apache releases, it's just what we currently use 
those with. 

Maven Repo
----------
Starting with release 0.3.12, i am also publishing release (and sometimes snapshot) artifacts. 
Here's repo url to use : 

   <repository>
      <id>dlyubimov-maven-repo-releases</id>
      <url>https://github.com/dlyubimov/dlyubimov-maven-repo/raw/master/releases</url>
     <snapshots>    
        <enabled>false</enabled>
      </snapshots>
      <releases>
        <enabled>true</enabled>
      </releases>
    </repository>
    <repository>
      <id>dlyubimov-maven-repo-snapshots</id>
      <url>https://github.com/dlyubimov/dlyubimov-maven-repo/raw/master/snapshots</url>
     <snapshots>    
        <enabled>true</enabled>
      </snapshots>
      <releases>
        <enabled>false</enabled>
      </releases>
    </repository>


License 
------- 
Apache 2.0


