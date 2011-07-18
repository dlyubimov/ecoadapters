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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.google.protobuf.Message;
import com.inadco.ecoadapters.EcoUtil;

/**
 * 
 * HBase Pig loader with schema support and protobuf conversion. somewhat
 * loosely based on pig's HBaseLoader.
 * <P>
 * 
 * CURRENT STATUS: working for trivial cases (but haven't checked for untrivial
 * ones.) OK to use with Pig 0.7.0, please report bugs.
 * <P>
 * 
 * Argument to the load func is as follows (using bnf per RFC-822):
 * 
 * <b><PRE>
 * 
 * col-specs = col-spec *(SPACE col-spec)   ; this is what you pass to the loader function
 * col-spec = col-family ":" col-name [":" protobuf-spec ]
 * protobuf-spec = protobuf-msg-class-name | protobuf-hdfs-spec 
 * protobuf-hdfs-spec = proto-desc-uri "?" "msg" "=" message-name
 * protobuf-msg-class-name = &lt;full java class name (with $ signs for inner classes) for the protobuf message to use to deserialize the cell value&gt;
 * proto-desc-uri = &lt;hdfs url pointing to protobuf descriptor file&gt;
 * </PRE></b>
 * 
 * <P>
 *
 * As it follows from above, there are 2 ways to specify 
 * protobuf message used to store a cell. <P> 
 * 
 * One way is to specify generated class name with <code>protobuf-msg-class-name</code>.<P>
 *   
 * Alternatively, protobuf-spec may be specified by protobuf descriptor file as
 * produced by protobuf compiler (at this point, 2.3.0) using
 * --descriptor_set_out option and uploaded to hdfs location specified by
 * <code>proto-desc-uri</code>.
 * <P>
 * 
 * The table name is passed in as the pig's LOAD string.
 * <P>
 * 
 * Example:
 * <P>
 * 
 * <B><pre>
 * CR = load 'CRAWLER_DATA' using 
 *      com.inadco.ecoadapters.pig.HBaseProtobufLoader(
 *     'contextrating:rating_2:$hdfs/data/inadco/protolib/inadco-logs.protodesc?msg=inadco.logs.ContentRating');
 * </pre></B><P>
 * 
 * the schema contains tuple filled with key and columns schemas (optionally
 * expanded based on protobuf message if specified), followed by their
 * timestamps:
 * <P>
 * 
 * <pre>
 * pig-schema = HKEY *(column1_schema column1_timestamp)
 * </pre>
 * 
 * For the example above the describe produces:<P>
 * 
 * <pre>
 * describe CR;
 *   
 *   CR: {HKEY: bytearray,contextrating::rating_2: (vendorId: int,contextRegressor: (xi: ... ),ERROR___: chararray),contextrating::rating_2::timestamp: long}
 * </pre><P>
 * 
 * As usual, ERROR___ is a pseudo column to contain stacktraces for
 * deserialization errors. (so one may count # of deserialization errors, for
 * example).
 * 
 * @author dmitriy
 * 
 */
public class HBaseProtobufLoader extends LoadFunc implements LoadMetadata {

    private static final Log LOG = LogFactory.getLog(HBaseProtobufLoader.class);

    private HBaseColSpec m_colSpec;
    
    private TupleFactory m_tupleFactory = TupleFactory.getInstance();

    private Configuration m_conf = new Configuration();
    private RecordReader<ImmutableBytesWritable, Result> m_reader;
    private Scan m_scan;

    /**
     * so we try to do simple parsing here . the column spec is the same as in
     * HBaseStorage except we add message type spec as per
     * {@link EcoUtil#inferDescriptorFromClassName(String)} or
     * {@link EcoUtil#inferDescriptorFromFilesystem(String)} definition.
     * 
     * @param colSpecStr
     * @throws PigException
     */
    public HBaseProtobufLoader(String colSpecStr) throws PigException {
        super();
        try {
            m_colSpec = new HBaseColSpec(colSpecStr,true);
            m_scan = new Scan();
            
            for (int i = 0; i < m_colSpec.m_cols.length; i++)
                m_scan.addColumn(m_colSpec.m_fams[i], m_colSpec.m_cols[i]);

        } catch (PigException exc) {
            throw exc;
        } catch (Throwable exc) {
            throw new PigException(exc);
        }
    }

    @Override
    public String[] getPartitionKeys(String arg0, Job arg1) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResourceSchema getSchema(String location, Job job)
            throws IOException {

        Schema ps = new Schema();

        ps.add(new FieldSchema(HBaseProtobufStorage.HKEY_ALIAS, DataType.BYTEARRAY)); // hbase key

        for (int i = 0; i < m_colSpec.m_pigSchema.length; i++) {
            String colName = Bytes.toString(m_colSpec.m_fams[i]) + "::"
                    + Bytes.toString(m_colSpec.m_cols[i]);
            if (m_colSpec.m_pigSchema[i] != null)
                ps.add(new FieldSchema(colName, m_colSpec.m_pigSchema[i], DataType.TUPLE));
            else
                ps.add(new FieldSchema(colName, DataType.BYTEARRAY));
            String timestampName = colName + "::timestamp";
            ps.add(new FieldSchema(timestampName, DataType.LONG));

        }

        return new ResourceSchema(ps);
    }

    @Override
    public ResourceStatistics getStatistics(String arg0, Job arg1)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setPartitionFilter(Expression arg0) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    @SuppressWarnings("rawtypes")
    public InputFormat getInputFormat() throws IOException {
        TableInputFormat inputFormat = new TableInputFormat();
        inputFormat.setConf(m_conf);
        return inputFormat;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            if (m_reader.nextKeyValue()) {

                Tuple tuple = TupleFactory.getInstance().newTuple();
                ImmutableBytesWritable key = (ImmutableBytesWritable) m_reader
                        .getCurrentKey();

                tuple.append(new DataByteArray(key.get(), key.getOffset(), key
                        .getLength()));

                Result result = (Result) m_reader.getCurrentValue();

                NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resMap = result
                        .getMap();

                for (int i = 0; i < m_colSpec.m_cols.length; ++i) {

                    NavigableMap<byte[], NavigableMap<Long, byte[]>> famMap = resMap
                            .get(m_colSpec.m_fams[i]);
                    if (famMap == null) {
                        tuple.append(null);
                        tuple.append(null);
                        continue;
                    }

                    NavigableMap<Long, byte[]> colMap = famMap.get(m_colSpec.m_cols[i]);
                    if (colMap == null) {
                        tuple.append(null);
                        tuple.append(null);
                        continue;
                    }

                    Entry<Long, byte[]> lastEntry = colMap.lastEntry();
                    if (lastEntry == null) {
                        tuple.append(null);
                        tuple.append(null);
                        continue;
                    }

                    byte[] val = lastEntry.getValue();
                    if (m_colSpec.m_msgBuilder[i] == null)
                        tuple.append(new DataByteArray(val));
                    else {
                        Message msg = m_colSpec.m_msgBuilder[i].clone().mergeFrom(val)
                                .build();
                        tuple.append(PigUtil.protoMessage2PigTuple(msg,
                                m_colSpec.m_msgDesc[i], m_tupleFactory));
                    }

                    tuple.append(lastEntry.getKey()); // the version
                }
                return tuple;
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        return null;
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {
        m_reader = reader;
    }

    static String HBASE_ZK_CLIENTPORT_PROP = "hbase.zookeeper.property.clientPort";
    static String HBASE_ZK_QUORUM_PROP = "hbase.zookeeper.quorum";

    @Override
    public void setLocation(String location, Job job) throws IOException {
        // if (location.startsWith("hbase://")){
        // m_conf.set(TableInputFormat.INPUT_TABLE, location.substring(8));
        // }else{
        // m_conf.set(TableInputFormat.INPUT_TABLE, location);
        // }
        int pos = location.lastIndexOf('/');
 
        // extract some configuration properties but not all of them , if
        // possible.
        Configuration jc = job.getConfiguration();
        String prop = jc.get(HBASE_ZK_CLIENTPORT_PROP);
        if (prop != null)
            m_conf.set(HBASE_ZK_CLIENTPORT_PROP, prop);
        prop = jc.get(HBASE_ZK_QUORUM_PROP);
        if (prop != null)
            m_conf.set(HBASE_ZK_QUORUM_PROP, prop);

        if (pos >= 0)
            m_conf.set(TableInputFormat.INPUT_TABLE,
                    location.substring(pos + 1));
        else
            m_conf.set(TableInputFormat.INPUT_TABLE, location);

        m_conf.set(TableInputFormat.SCAN, convertScanToString(m_scan));
    }

    private static String convertScanToString(Scan scan) {

        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            scan.write(dos);
            return Base64.encodeBytes(out.toByteArray());
        } catch (IOException e) {
            LOG.error(e);
            return "";
        }

    }

}
