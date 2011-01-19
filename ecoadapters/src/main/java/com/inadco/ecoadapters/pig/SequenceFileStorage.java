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
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/** 
 * pig storage using compressed sequence files 
 * compatible with our ecoadapter loaders 
 * 
 * Updated for pig 0.7.0 interfaces
 *  
 * @author dmitriy
 *
 */
public class SequenceFileStorage extends StoreFunc  {

    @SuppressWarnings("unused")
    private static final Log LOG = LogFactory
    .getLog(SequenceFileStorage.class);

    

    private Text                    m_key = new Text();
    private BytesWritable           m_value = new BytesWritable();
    
    private SequenceFileOutputFormat<Text, BytesWritable> m_outputFormat ;
    private RecordWriter<Text, BytesWritable>   m_recordWriter;
    

    public SequenceFileStorage() {
        super();
        m_outputFormat = new SequenceFileOutputFormat<Text, BytesWritable>();
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
            if ( f.size()!= 1 ) 
                throw new IOException ("wrong type of tuple attributes, expected exactly 1 while storing into sequence file");
            if ( f.getType(0)!= DataType.BYTEARRAY)
                throw new IOException ( "wrong type of the stored value, expected byte array while storing into sequence file");
            DataByteArray dba = (DataByteArray) f.get(0);
            m_value.set(dba.get(), 0, dba.size());
            m_recordWriter.write(m_key, m_value);
        } catch ( InterruptedException exc ) { 
            throw new IOException ( exc ) ; 
        }
    }

    

}
