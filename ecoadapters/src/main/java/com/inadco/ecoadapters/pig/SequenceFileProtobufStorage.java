package com.inadco.ecoadapters.pig;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.Descriptor;
import com.inadco.ecoadapters.EcoUtil;
import com.inadco.ecoadapters.pig.PigUtil.Tuple2ProtoMap;

/**
 * try to combine protobuf mapping & store steps .
 * 
 * There's a potential problem if more than 1 function 
 * is used in the same MR job (if pig handles that as multiple outputs) . <P>
 * 
 * if that's the case, to schema passing should include 
 * distinguishing schemas for message name saved. This should be relatively 
 * simple fix (e.g. something like using properties in the form 
 * 'inadco.SequenceFileProtobufStorage.<protoMsgName>.schema=<schema string>'. <P> 
 * 
 * 
 * @author dmitriy
 *
 */

public class SequenceFileProtobufStorage extends StoreFunc {
    private static final Log LOG = LogFactory
    .getLog(SequenceFileStorage.class);

    private static final String SCHEMA_PROPERTY="inadco.SequenceFileProtobufStorage.schema.";
    

    private Text                    m_key = new Text();
    private BytesWritable           m_value = new BytesWritable();
    
    private SequenceFileOutputFormat<Text, BytesWritable> m_outputFormat ;
    private RecordWriter<Text, BytesWritable>   m_recordWriter;
    
    private String                  m_msgDescString;
    private Schema                  m_pigSchema;
    private Descriptor              m_msgDesc;
    private Message.Builder         m_msgBuilder;
    private Tuple2ProtoMap          m_tuple2ProtoMap;
    

    public SequenceFileProtobufStorage(String msgDescString ) {
        super();
        m_outputFormat = new SequenceFileOutputFormat<Text, BytesWritable>();
        m_msgDescString=msgDescString;
    }

    

    @Override
    @SuppressWarnings("rawtypes")
    public OutputFormat getOutputFormat() throws IOException {
        return m_outputFormat;
    }



    @Override
    @SuppressWarnings({"rawtypes","unchecked"})
    public void prepareToWrite(RecordWriter writer) throws IOException {
        m_recordWriter=writer;
        
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        SequenceFileOutputFormat.setOutputPath(job, new Path(location));
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        
    }

    @Override
    public void putNext(Tuple f) throws IOException {
        try { 
            if ( m_tuple2ProtoMap == null ) _initMessageDesc(m_msgDescString);
            Message.Builder bldr=m_msgBuilder.clone();
            
            PigUtil.pigTuple2ProtoMessage(f, m_tuple2ProtoMap, bldr, 0, f.size());
            byte[] val = bldr.build().toByteArray();
            m_value.set(val,0,val.length);
            
//            if ( f.size()!= 1 ) 
//                throw new IOException ("wrong type of tuple attributes, expected exactly 1 while storing into sequence file");
//            if ( f.getType(0)!= DataType.BYTEARRAY)
//                throw new IOException ( "wrong type of the stored value, expected byte array while storing into sequence file");
//            DataByteArray dba = (DataByteArray) f.get(0);
//            m_value.set(dba.get(), 0, dba.size());
            m_recordWriter.write(m_key, m_value);
        } catch ( IOException exc ) { 
            throw exc;
        } catch ( Throwable exc ) { 
            throw new IOException ( exc ) ; 
        }
    }



    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        m_pigSchema=Schema.getPigSchema(s);
        UDFContext udfctx= UDFContext.getUDFContext();
        udfctx.getUDFProperties(SequenceFileProtobufStorage.class).setProperty(
                SCHEMA_PROPERTY+m_msgDescString, PigUtil.stringifySchema(m_pigSchema));
        
        // just for the sake of validation, try to build map on front end before pushing it all to backend.
        _initMessageDesc(m_msgDescString);
    }

    
    private void _initMessageDesc (String msgDescString ) throws PigException {
        try { 
            
            if ( m_pigSchema==null ) { 
                // backend . try to load it back from properties. 
                UDFContext udfctx = UDFContext.getUDFContext(); 
                String schemaStr = udfctx.getUDFProperties(SequenceFileProtobufStorage.class).getProperty(SCHEMA_PROPERTY+m_msgDescString);
                
                if ( schemaStr ==null ) 
                    throw new PigException ( "Pig schema required for input of this function, wasn't set.");
                m_pigSchema = PigUtil.destringifySchema(schemaStr);
                
            }
            
            if ( msgDescString.startsWith("hdfs://"))
                m_msgDesc=EcoUtil.inferDescriptorFromFilesystem(msgDescString);
            else m_msgDesc = EcoUtil.inferDescriptorFromClassName(msgDescString);
            m_msgBuilder = DynamicMessage.newBuilder(m_msgDesc);
            m_tuple2ProtoMap=PigUtil.generatePigTuple2ProtoMap(m_pigSchema, m_msgDesc);
            
    
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("Loaded LoadFunc for message class:%s",
                        msgDescString));
        } catch ( PigException exc  ) {
            throw exc;
        } catch ( Throwable thr ) { 
            throw new PigException ( thr ) ; 
        } 
    }

    
}
