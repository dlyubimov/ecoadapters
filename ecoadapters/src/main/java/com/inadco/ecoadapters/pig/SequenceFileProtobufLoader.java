package com.inadco.ecoadapters.pig;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.Descriptor;
import com.inadco.ecoadapters.EcoUtil;

/**
 * This is loosely based SequenceFileLoader that is provided with pig
 * contributions. it extracts sequence file values (same way Hive does) and
 * decodes them according to the schema supplied. of course we do protobuf parsing based on 
 * dynamic message . <p> 
 * 
 * Use example: <p>
 * 
 * <pre>

register protobuf-java-2.3.0.jar;
register inadco-protolib-1.0-SNAPSHOT.jar;

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


 </pre>
 * 
 * @author Dmitriy
 **/

public class SequenceFileProtobufLoader extends LoadFunc implements LoadMetadata {

	private static final Log LOG = LogFactory
			.getLog(SequenceFileProtobufLoader.class);

	protected TupleFactory 			m_tupleFactory = TupleFactory.getInstance();

	private Descriptor 				m_msgDesc;
	private Message.Builder			m_msgBuilder;
	private Schema 					m_pigSchema;

	private InputFormat<Text,BytesWritable>        m_inputFormat;
	private RecordReader<Text, BytesWritable>      m_recordReader;

	public SequenceFileProtobufLoader(String msgDescString) {

		try {
//			if ( msgDescString.startsWith("hdfs://"))
//				m_msgDesc=EcoUtil.inferDescriptorFromFilesystem(msgDescString);
//			else m_msgDesc = EcoUtil.inferDescriptorFromClassName(msgDescString);
			if (LOG.isDebugEnabled()){
				LOG.debug("Message Desc String:" + msgDescString);
			}
			m_msgDesc = parseMsgDesc(msgDescString);
			m_msgBuilder = DynamicMessage.newBuilder(m_msgDesc);
			m_pigSchema = PigUtil.generatePigSchemaFromProto(m_msgDesc);
			
			m_inputFormat =new SequenceFileInputFormat<Text, BytesWritable>();

//			if (LOG.isDebugEnabled())
//				LOG.debug(String.format("Loaded LoadFunc for message class:%s",
//						msgDescString));

		} catch (Throwable thr) {
			if (thr instanceof RuntimeException)
				throw (RuntimeException) thr;
			else
				throw new RuntimeException(thr);
		}
	}
	public static Descriptor parseMsgDesc(String msgDescString) throws Throwable{
		if ( msgDescString.startsWith("hdfs://"))
			return EcoUtil.inferDescriptorFromFilesystem(msgDescString);
		else return EcoUtil.inferDescriptorFromClassName(msgDescString);
	}
	
	@Override
	@SuppressWarnings("rawtypes")
    public InputFormat getInputFormat() throws IOException {
	    return m_inputFormat;
    }


	@Override
    public String[] getPartitionKeys(String location, Job job) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {
        return new ResourceSchema(m_pigSchema);
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setPartitionFilter(Expression expr) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    @SuppressWarnings({"rawtypes","unchecked"})
    public void prepareToRead(RecordReader reader, PigSplit psplit)
            throws IOException {
        m_recordReader=reader;
        
    }



    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location); // is that what they mean?
    }



    @Override
	public Tuple getNext() throws IOException {
        
        try { 
            if ( ! m_recordReader.nextKeyValue())
                return null; 
                
            BytesWritable bw=m_recordReader.getCurrentValue(); 
    
            Message msg = m_msgBuilder.clone().mergeFrom(bw.getBytes(),0,bw.getLength()).buildPartial();
    		return PigUtil.protoMessage2PigTuple(msg, m_msgDesc, m_tupleFactory );
        } catch ( InterruptedException exc ) { 
            throw new IOException ( exc );
        } catch ( IOException exc ) { 
            return reportError(exc);
        }
		
	}

	
	private Tuple reportError ( Throwable thr ) { 
		Tuple msgTuple = m_tupleFactory.newTuple();
		int errInd= m_msgDesc.getFields().size();
		for ( int i =0; i < errInd; i++ ) msgTuple.append(null);
		
		StringWriter stw = new StringWriter();
		PrintWriter pw = new PrintWriter ( stw);
		thr.printStackTrace(pw);
		pw.close();
		
		msgTuple.append ( thr.getMessage()+"\n"+stw.toString());
		
		return msgTuple;
		
	}

}
