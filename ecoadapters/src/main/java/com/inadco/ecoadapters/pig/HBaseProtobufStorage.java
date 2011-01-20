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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.inadco.ecoadapters.pig.PigUtil.Tuple2ProtoMap;

/**
 * 
 * See {@link HBaseProtobufLoader} and {@link SequenceFileProtobufStorage} to
 * get an idea what is happening here.
 * <P>
 * 
 * @author dmitriy
 * 
 */
public class HBaseProtobufStorage extends StoreFunc {

    private static final String SCHEMA_PROPERTY = "inadco.HBaseProtobufStorage.schema";
    static final String HKEY_ALIAS = "HKEY";

    private HBaseColSpec m_colSpec;
    private Schema m_pigSchema;
    private int[] m_colPigSchemaIndices;
    private int   m_keyPigSchemaIndex;
    private Tuple2ProtoMap[] m_protoMaps;

    /**
     * 
     * @param colSpecString
     *            Same format column specification as in
     *            {@link HBaseProtobufLoader#HBaseProtobufLoader(String)}.
     */

    public HBaseProtobufStorage(String colSpecString) throws PigException {
        super();
        m_colSpec = new HBaseColSpec(colSpecString, false);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public OutputFormat getOutputFormat() throws IOException {
        TableOutputFormat tof = new TableOutputFormat<Object>();
        return tof;

    }

    @Override
    public void prepareToWrite(RecordWriter rw) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void putNext(Tuple t) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        m_pigSchema = Schema.getPigSchema(s);

        UDFContext udfctx = UDFContext.getUDFContext();
        udfctx.getUDFProperties(SequenceFileProtobufStorage.class).setProperty(
                SCHEMA_PROPERTY, PigUtil.stringifySchema(m_pigSchema));

        for (int i = 0; i < m_colSpec.m_cols.length; i++) {
            String hbaseCol = Bytes.toString(m_colSpec.m_cols[i]);
            if (m_pigSchema.getField(hbaseCol) == null)
                throw new IOException(String.format(
                        "Unable to map output column %s in pig output. ",
                        hbaseCol));
        }
        
        _init();

    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        int pos = location.lastIndexOf('/');

        // extract some configuration properties but not all of them , if
        // possible.
        // Configuration jc = job.getConfiguration();
        // String prop = jc.get(HBaseProtobufLoader.HBASE_ZK_CLIENTPORT_PROP);
        // if (prop != null)
        // m_conf.set(HBaseProtobufLoader.HBASE_ZK_CLIENTPORT_PROP, prop);
        // prop = jc.get(HBaseProtobufLoader.HBASE_ZK_QUORUM_PROP);
        // if (prop != null)
        // m_conf.set(HBaseProtobufLoader.HBASE_ZK_QUORUM_PROP, prop);

        if (pos >= 0)
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,
                    location.substring(pos + 1));
        else
            job.getConfiguration()
                    .set(TableOutputFormat.OUTPUT_TABLE, location);

    }

    private void _init() throws IOException {
        try {
            if (m_pigSchema == null) {
                // backend . try to load it back from properties.
                UDFContext udfctx = UDFContext.getUDFContext();
                String schemaStr = udfctx.getUDFProperties(
                        SequenceFileProtobufStorage.class).getProperty(
                        SCHEMA_PROPERTY);

                if (schemaStr == null)
                    throw new PigException(
                            "Pig schema required for input of this function, wasn't set.");
                m_pigSchema = PigUtil.destringifySchema(schemaStr);

            }
            
            // remap columns to schema positional indices 
            m_colPigSchemaIndices=new int[m_colSpec.m_cols.length];
            m_protoMaps=new Tuple2ProtoMap[m_colSpec.m_cols.length];
            for ( int i = 0; i < m_colSpec.m_cols.length; i++ ) { 
                String col=Bytes.toString(m_colSpec.m_cols[i]);
                int pos =m_pigSchema.getPosition(col);
                if ( pos <0 ) 
                    throw new IOException ( String.format (
                            "column %s is not found in the pig output.",
                            col
                            ));
                m_colPigSchemaIndices[i]=pos;
                if ( m_colSpec.m_msgDesc[i]!=null) { // this column is a proto 
                    FieldSchema fs = m_pigSchema.getField(pos);
                    if ( fs.type!= DataType.TUPLE ) 
                        throw new IOException (String.format(
                                "Tuple is expected for pig output attribute %s.",
                                col));
                    m_protoMaps[i]=PigUtil.generatePigTuple2ProtoMap(fs.schema, m_colSpec.m_msgDesc[i]);
                }
            }
            m_keyPigSchemaIndex=m_pigSchema.getPosition(HKEY_ALIAS);
            if ( m_keyPigSchemaIndex<0 ) 
                throw new IOException ( String.format(
                        "Unable to find pig field by name %s which is expected to be output key.", 
                        HKEY_ALIAS
                        ));
        } catch (IOException exc) {
            throw exc;
        } catch (Throwable t) {
            throw new IOException(t);
        }

    }

}
