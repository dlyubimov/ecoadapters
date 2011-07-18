/**                                                                                        
 *                                                                                         
 *  Copyright © 2010, 2011 Inadco, Inc. All rights reserved.                               
 *                                                                                         
 *     Licensed under the Apache License, Version 2.0 (the "License");                     
 *     you may not use this file except in compliance with the License.                    
 *     You may obtain a copy of the License at                                             
 *                                                                                         
 *         http://www.apache.org/licenses/LICENSE-2.0                                      
 *                                                                                         
 *     Unless required by applicable law or agreed to in writing, software                 
 *     distributed under the License is distributed on an "AS IS" BASIS,                   
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.            
 *     See the License for the specific language governing permissions and                 
 *     limitations under the License.                                                      
 *                                                                                         
 *                                                                                         
 */
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
 * {@link StoreFunc} for converting pig output into protobuf messages and saving
 * them into sequence files. The process is symmetric to deserializing messages:
 * incoming pig schema is scanned for attributes with the same names as protobuf
 * message descriptor and then appropriate type conversion is applied at runtime
 * (deep conversion). During schema mapping, it is an error to have a pig
 * attribute with a name that is not found in the protobuf descriptor of the
 * message (i.e. 100% of incoming attributes must map to their protobuf
 * counterparts).
 * <P>
 * 
 * There's a potential problem if more than 1 function is used in the same MR
 * job (if pig handles that as multiple outputs) .
 * <P>
 * 
 * if that's the case, to schema passing should include distinguishing schemas
 * for message name saved. This should be relatively simple fix (e.g. something
 * like using properties in the form
 * 'inadco.SequenceFileProtobufStorage.<protoMsgName>.schema=<schema string>'.
 * <P>
 * 
 * It is also possible that this could be rectified by <code>define</code>
 * although i did not try it.
 * <P>
 * 
 * 
 * @author dmitriy
 * 
 */

public class SequenceFileProtobufStorage extends StoreFunc {
    private static final Log                              LOG             = LogFactory
                                                                              .getLog(SequenceFileStorage.class);

    private static final String                           SCHEMA_PROPERTY = "inadco.SequenceFileProtobufStorage.schema";

    private Text                                          m_key           = new Text();
    private BytesWritable                                 m_value         = new BytesWritable();

    private SequenceFileOutputFormat<Text, BytesWritable> m_outputFormat;
    private RecordWriter<Text, BytesWritable>             m_recordWriter;

    private String                                        m_msgDescString;
    private Schema                                        m_pigSchema;
    private Descriptor                                    m_msgDesc;
    private Message.Builder                               m_msgBuilder;
    private Tuple2ProtoMap                                m_tuple2ProtoMap;

    public SequenceFileProtobufStorage(String msgDescString) {
        super();
        m_outputFormat = new SequenceFileOutputFormat<Text, BytesWritable>();
        m_msgDescString = msgDescString;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public OutputFormat getOutputFormat() throws IOException {
        return m_outputFormat;
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void prepareToWrite(RecordWriter writer) throws IOException {
        m_recordWriter = writer;

    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        SequenceFileOutputFormat.setOutputPath(job, new Path(location));
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
        // this now comes from default configuration property, no need to hardcode this, really. 
        
//        SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

    }

    @Override
    public void putNext(Tuple f) throws IOException {
        try {
            if (m_tuple2ProtoMap == null)
                _initMessageDesc(m_msgDescString); // lazy 
            
            Message.Builder bldr = m_msgBuilder.clone();

            PigUtil.pigTuple2ProtoMessage(f, m_tuple2ProtoMap, bldr, 0, f.size());
            byte[] val = bldr.build().toByteArray();
            m_value.set(val, 0, val.length);

            // if ( f.size()!= 1 )
            // throw new IOException
            // ("wrong type of tuple attributes, expected exactly 1 while storing into sequence file");
            // if ( f.getType(0)!= DataType.BYTEARRAY)
            // throw new IOException (
            // "wrong type of the stored value, expected byte array while storing into sequence file");
            // DataByteArray dba = (DataByteArray) f.get(0);
            // m_value.set(dba.get(), 0, dba.size());
            m_recordWriter.write(m_key, m_value);
        } catch (IOException exc) {
            throw exc;
        } catch (Throwable exc) {
            throw new IOException(exc);
        }
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        m_pigSchema = Schema.getPigSchema(s);
        UDFContext udfctx = UDFContext.getUDFContext();
        udfctx.getUDFProperties(SequenceFileProtobufStorage.class).setProperty(SCHEMA_PROPERTY,
                                                                               PigUtil.stringifySchema(m_pigSchema));

        // just for the sake of validation, try to build map on front end before
        // pushing it all to backend.
        _initMessageDesc(m_msgDescString);
    }

    private void _initMessageDesc(String msgDescString) throws PigException {
        try {

            if (m_pigSchema == null) {
                // backend . try to load it back from properties.
                UDFContext udfctx = UDFContext.getUDFContext();
                String schemaStr =
                    udfctx.getUDFProperties(SequenceFileProtobufStorage.class).getProperty(SCHEMA_PROPERTY);

                if (schemaStr == null)
                    throw new PigException("Pig schema required for input of this function, wasn't set.");
                m_pigSchema = PigUtil.destringifySchema(schemaStr);

            }

            if (msgDescString.startsWith("hdfs://"))
                m_msgDesc = EcoUtil.inferDescriptorFromFilesystem(msgDescString);
            else
                m_msgDesc = EcoUtil.inferDescriptorFromClassName(msgDescString);
            m_msgBuilder = DynamicMessage.newBuilder(m_msgDesc);
            m_tuple2ProtoMap = PigUtil.generatePigTuple2ProtoMap(m_pigSchema, m_msgDesc);

            if (LOG.isDebugEnabled())
                LOG.debug(String.format("Loaded LoadFunc for message class:%s", msgDescString));
        } catch (PigException exc) {
            throw exc;
        } catch (Throwable thr) {
            throw new PigException(thr);
        }
    }

}
