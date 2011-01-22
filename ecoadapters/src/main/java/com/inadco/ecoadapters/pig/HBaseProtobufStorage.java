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

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
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

import com.inadco.ecoadapters.pig.PigUtil.Tuple2ProtoMap;

/**
 * 
 * See {@link HBaseProtobufLoader} and {@link SequenceFileProtobufStorage} to
 * get an idea what is happening here.
 * <P>
 * 
 * When we save pig output, we match each column in the hbase output specification 
 * to the pig output field with the same name. If hbase output specification 
 * specifies protobuf conversion, the pig output field is expected to be a pig tuple. 
 * Otherwise, we support pig byte arrays (no conversion) or pig chararray type 
 * (converting to byte array using utf-8 encoding).<P> 
 * 
 * We also expected pig field by name {@link #HKEY_ALIAS} to be used as hbase key. 
 * The conversions available for that are the same as for non-protobuf columns, 
 * namely, pig bytearray or pig chararray.<P>
 * 
 * Finally, it is prudent to note that if a mapped pig output contains null, we issue a 
 * {@link Delete} (otherwise we do a normal {@link Put}). Thus, every output may result 
 * in either {@link Put} or {@link Delete} or both.<P> 
 * 
 * It is fatal to return a null for the {@link #HKEY_ALIAS} pig field.
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
    private PigUtil.Pig2HBaseStrategy[] m_colConvStrategies;
    private PigUtil.Pig2HBaseStrategy m_keyConvStrategy;
    private RecordWriter<NullWritable, Object> m_recordWriter;

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

    @SuppressWarnings({"unchecked","rawtypes"})
    @Override
    public void prepareToWrite(RecordWriter rw) throws IOException {
        m_recordWriter=rw;
    }

    @Override
    public void putNext(Tuple t) throws IOException {
        Put put=null; 
        Delete delete=null;
        
        if ( m_keyConvStrategy==null ) _init(); // lazy backend init
        
        byte[] key=m_keyConvStrategy.toHbase(t.get(m_keyPigSchemaIndex));
        if ( key == null ) 
            throw new IOException ( "null value for hbase key attribute encountered in the pig output.");

        for ( int i = 0; i< m_colConvStrategies.length; i++  ) { 
            byte[] colval=m_colConvStrategies[i].toHbase(t.get(m_colPigSchemaIndices[i]));
            if ( colval != null ) { 
                if ( put == null )  
                    put = new Put(key);
                put.add(m_colSpec.m_fams[i], m_colSpec.m_cols[i], colval);
            } else { 
                if ( delete==null ) 
                    delete = new Delete(key);
                delete.deleteColumn(m_colSpec.m_fams[i],m_colSpec.m_cols[i]);
            }
        }

        try { 
            if ( put != null ) m_recordWriter.write(NullWritable.get(), put);
            if ( delete != null ) m_recordWriter.write(NullWritable.get(),delete);
        } catch  ( InterruptedException exc ) { 
            throw new IOException("interrupted",exc);
        }

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
            
            // m_protoMaps=new Tuple2ProtoMap[m_colSpec.m_cols.length];
            m_colConvStrategies=new PigUtil.Pig2HBaseStrategy[m_colSpec.m_cols.length];
            
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
                    Tuple2ProtoMap map=PigUtil.generatePigTuple2ProtoMap(fs.schema, m_colSpec.m_msgDesc[i]);
                    m_colConvStrategies[i]=new PigUtil.PigTuple2HBaseConversion(map, m_colSpec.m_msgBuilder[i]);
                    
                } else { 
                    switch ( m_pigSchema.getField(pos).type) { 
                    case DataType.BYTEARRAY:
                        m_colConvStrategies[i]=new PigUtil.PigByteArray2HBaseConversion();
                        break;
                    case DataType.CHARARRAY:
                        m_colConvStrategies[i]=new PigUtil.PigCharArray2HBaseConversion();
                        break;
                    default:
                        throw new IOException ( String.format (
                                "Unexpected/unsupported pig type for pig output for column %s.", 
                                col
                                ));
                    }
                }
            }
            m_keyPigSchemaIndex=m_pigSchema.getPosition(HKEY_ALIAS);
            if ( m_keyPigSchemaIndex<0 ) 
                throw new IOException ( String.format(
                        "Unable to find pig field by name %s which is expected to be output key.", 
                        HKEY_ALIAS
                        ));
            switch ( m_pigSchema.getField(m_keyPigSchemaIndex).type) { 
            case DataType.BYTEARRAY:
                m_keyConvStrategy=new PigUtil.PigByteArray2HBaseConversion();
                break;
            case DataType.CHARARRAY:
                m_keyConvStrategy=new PigUtil.PigCharArray2HBaseConversion();
                break;
            default:
                throw new IOException ( String.format (
                        "Unexpected/unsupported pig type for pig output field %s which is supposed to be the table key.", 
                        HKEY_ALIAS
                        ));
            }
        } catch (IOException exc) {
            throw exc;
        } catch (Throwable t) {
            throw new IOException(t);
        }

    }

}
