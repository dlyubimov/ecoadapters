package com.inadco.ecoadapters.hive;


import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.inadco.ecoadapters.EcoUtil;


/**
 * supports {@link SerDe} for protobuf-packed messages 
 * inside {@link SequenceFile}s.<P>
 * 
 * sequence files must have protobuff messages saved as {@link BytesWritable} values.
 * Since protobuf doesn't really include type information along with the message, 
 * the message type must be set for the entire table. Example: <P>
 * <pre> 
CREATE EXTERNAL TABLE IMPRESSION_LOGS 
ROW FORMAT SERDE 'com.inadco.ecoadapters.hive.ProtobufSerDe'
WITH SERDEPROPERTIES ("messageClass"="com.inadco.logging.codegen.test.TestMessages$TestLogProto")
STORED AS SEQUENCEFILE
LOCATION '/data/inadco/var/log/IMPRESSION'

Found class for com.inadco.ecoadapters.hive.ProtobufSerDe
OK
Time taken: 3.175 seconds

 * </pre>
 * 
 * -or - <P>
 * 
 * another way to connect hive to external table is to specify 
 * the protodesc file (locally or on hdfs as output by protoc with --descriptor_set_out option) 
 * for the files. This way it doesn't 
 * require anything but protobuf-java.jar in the $HIVE_HOME/auxlib and doesn't 
 * require recompilation of any java code:<P>
 * 
 *  <pre>
CREATE EXTERNAL TABLE IMPRESSION_LOGS 
ROW FORMAT SERDE 'com.inadco.ecoadapters.hive.ProtobufSerDe'
WITH SERDEPROPERTIES ("fileDescSetUri"="hdfs://localhost:11010/data/inadco/protolib/testMessages.protodesc?msg=inadco.test.TestLogProto")
STORED AS SEQUENCEFILE
LOCATION '/data/inadco/var/log/IMPRESSION'

 *  </pre>
 * 
 *  the protobuf messages are mapped as hive 'structures', 
 *  repeated fields are mapped as hive 'arrays' 
 *  and each field has its name from the message definition . <P>
 *  
 *  Note that it appears that we don't have to specify 
 *  column names in the table definition. in the example above, 
 *  here's what hive knows about that table (as of the time of this writing): <P>
 *  <pre>
hive> describe impression_logs;
OK
landingpagetitle        string  from deserializer
landingpagekeyword      string  from deserializer
uniqueurl       string  from deserializer
isdelete        boolean from deserializer
isnew   boolean from deserializer
isdirty boolean from deserializer
___error___ string  from deserializer
Time taken: 0.125 seconds

 *  </pre>
 * 
 * each protobuf message also has an implicit column '__serde_error__' which is going to 
 * be populated with any deserialization errors that happen during processing. 
 * e.g. we can count how many deserialization errors we have in the logs: 
 * 
 * <pre>
hive> select count(`__serde_error__`), count(1) from impression_logs;
Ended Job = job_201004191323_0001
OK
0       10000
Time taken: 24.724 seconds
 * </pre>
 * so 0 errors out of 10000 records.<P>
 * 
 * Also note that serialization (i.e. building & writing protobuffer messages) is not expected to work.<P>
 * 
 * @author dmitriy
 *
 */
public class ProtobufSerDe implements SerDe {
    
    private static final Logger      	LOG = Logger.getLogger(ProtobufSerDe.class); 

    private Descriptors.Descriptor  	m_msgDesc;
    private StructObjectInspector   	m_protoMsgInspector;
    private Message.Builder         	m_msgBuilder;
//    private Class<? extends Message> 	m_msgClass;
    
    
    public ProtobufSerDe() {
        super();
    }
    @Override
    
    public void initialize(Configuration configuration, Properties props)
            throws SerDeException {
        
        try { 
        	String msgClsName=props.getProperty("messageClass");
        	String fileDescSetUri=props.getProperty("fileDescSetUri");
            if ( msgClsName != null ) 
            	m_msgDesc=EcoUtil.inferDescriptorFromClassName(msgClsName);
            else if ( fileDescSetUri!= null ) {
            	m_msgDesc=EcoUtil.inferDescriptorFromFilesystem(fileDescSetUri);
            	if ( m_msgDesc == null ) 
            		throw new SerDeException(
            				String.format ( 
            						"Unable to locate message in the protodesc file identified by uri %s", 
            						fileDescSetUri));
            } else throw new SerDeException("either FileDescriptorSet's file uri or message class name must be specified.");
            
            m_msgBuilder=DynamicMessage.newBuilder(m_msgDesc);
            m_protoMsgInspector=ProtobufMessageObjectInspectorFactory.createProtobufObjectInspectorFor(m_msgDesc);
            
            
            if ( LOG.isDebugEnabled())  
                LOG.debug(String.format ( 
                        "Loaded SerDe for message '%s'.", m_msgDesc.getName()));
            
        } catch ( SerDeException exc ) { 
            throw exc; 
        } catch ( Throwable thr ) { 
            throw new SerDeException(thr);
        }
    }
    

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return BytesWritable.class;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return m_protoMsgInspector;
    }

    @Override
    public Object deserialize(Writable writableBlob) throws SerDeException {
        
        try { 
            BytesWritable value=(BytesWritable)writableBlob;
            
            byte[] msgBytes= Arrays.copyOf(value.getBytes(), value.getLength());
            Message msg=  m_msgBuilder.clone().mergeFrom(msgBytes).buildPartial();
//            DynamicMessage dmsg=DynamicMessage.parseFrom(m_msgDesc, msgBytes);
            return parseMessage(msg,m_msgDesc);
            
            
        } catch ( Throwable thr ) { 
            return generateError(thr);
        }
    }

    @Override
    public Writable serialize(Object src, ObjectInspector oi)
            throws SerDeException {
    	throw new SerDeException ("serialization into a protobuf object not yet supported");
    }
    


    private static List<Object> parseMessage(Message msg, Descriptors.Descriptor desc ) { 
        List<Object> result=new ArrayList<Object>();
        for ( FieldDescriptor fd :desc.getFields()) { 
            if ( fd.isRepeated() )  
                result.add(parseRepeatedField(msg, fd)) ;
            else { 
                if ( msg.hasField(fd) ) result.add(parseNonRepeated(msg,fd));
                else result.add(null);   
            } 
        }
        result.add(null); // no error
        return result;
    }
    private List<Object> generateError ( Throwable thr  ){
        
        List<Object> result = new ArrayList<Object>();
        
        for ( int i =0; i < m_protoMsgInspector.getAllStructFieldRefs().size()-1; i++ )
            result.add(null);
        
        // TODO: include full stack trace, i guess
        String error = thr.toString();
        // first field is always the error 
        result.add(error);
        
        return result;
        
    }
    
    private static Object parseRepeatedField ( Message msg, FieldDescriptor fd ) {
        List<Object> result = new ArrayList<Object>();
        int cnt = msg.getRepeatedFieldCount(fd);
        for ( int i =0; i < cnt; i++) 
            result.add(convertType(msg.getRepeatedField(fd, i), fd));
        return result;
    }
    
    private static Object parseNonRepeated ( Message msg, FieldDescriptor fd ) {
        return convertType(msg.getField(fd),fd);
    }
    
    private static Object convertType ( Object src, FieldDescriptor fd ) {
        
        switch ( fd.getType()) { 
        case MESSAGE:
            Message msgField=(Message)src;
            return parseMessage(msgField, fd.getMessageType());
        case BYTES: 
            byte[] val=((ByteString) src).toByteArray();
            // convert to hex string-- short ones only -- hive doesn't really support much else 
            return String.format("%X", new BigInteger(1,val));
        case BOOL: 
        case DOUBLE:  
        
        case SINT32:
        case SFIXED32:
        case UINT32:
        case INT32:
        case FIXED32: 
        case SINT64:
        case SFIXED64:
        case INT64:
        case UINT64:
        case FIXED64: 
        case FLOAT:
        case STRING:
            return src;
        
        case ENUM: // convert to strings.
            EnumValueDescriptor enDesc=(EnumValueDescriptor) src;
            return enDesc.getName();
            
        case GROUP:
        
        default : throw new UnsupportedOperationException();            
            
        }
    }

}
