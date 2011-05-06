package com.inadco.ecoadapters.pig;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;

import com.google.protobuf.Message;

/**
 * Do things similar to HBaseProtobufLoader, only yank some values in realtime.
 * <P>
 * 
 * It is not expected to be terribly efficient since it is not using batches or
 * scan, but oh well.
 * <P>
 * 
 * use
 * 
 * <pre>
 * define getMyTab com.inadco.ecoadapters.pig.HBaseGet(mytab,mycolspec)
 * </pre>
 * 
 * . Colspec has the same format as in {@link HBaseProtobufLoader}.
 * <P>
 * 
 * Accepts single argument: byte array signifying hbase key. The difference in
 * output schema compared to {@link HBaseProtobufLoader} is that it is not going
 * to have key output, only the requested columns.
 * <P>
 * 
 * Status: initial development.
 * 
 * @author dmitriy
 * 
 */

public class HBaseGet extends EvalFunc<Tuple> {

    private byte[]       m_table;
    private HBaseColSpec m_colSpec;
    private HTable       m_htable;
    private TupleFactory m_tupleFactory;

    public HBaseGet(String table, String colspec) throws PigException {
        super();
        m_table = Bytes.toBytes(table);
        m_colSpec = new HBaseColSpec(colspec, true);

    }

    @Override
    public Schema outputSchema(Schema input) {
        Schema ps = new Schema();

        try {

            // validate input

            // generate output schema.
            for (int i = 0; i < m_colSpec.m_pigSchema.length; i++) {
                String colName = Bytes.toString(m_colSpec.m_fams[i]) + "::" + Bytes.toString(m_colSpec.m_cols[i]);
                if (m_colSpec.m_pigSchema[i] != null)
                    ps.add(new FieldSchema(colName, m_colSpec.m_pigSchema[i], DataType.TUPLE));
                else
                    ps.add(new FieldSchema(colName, DataType.BYTEARRAY));
                String timestampName = colName + "::timestamp";
                ps.add(new FieldSchema(timestampName, DataType.LONG));

            }
            return ps;
        } catch (FrontendException exc) {
            throw new IllegalArgumentException(exc);
        }
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (input == null)
            return null;
        if (m_htable == null)
            _initHtable();
        Get get = new Get(DataType.toBytes(input.get(0)));
        for (int i = 0; i < m_colSpec.m_cols.length; i++)
            get.addColumn(m_colSpec.m_fams[i], m_colSpec.m_cols[i]);
        Result result = m_htable.get(get);
        if (result.isEmpty())
            return null;

        Tuple tuple = TupleFactory.getInstance().newTuple();

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resMap = result.getMap();

        for (int i = 0; i < m_colSpec.m_cols.length; ++i) {

            NavigableMap<byte[], NavigableMap<Long, byte[]>> famMap = resMap.get(m_colSpec.m_fams[i]);
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
                Message msg = m_colSpec.m_msgBuilder[i].clone().mergeFrom(val).build();
                tuple.append(PigUtil.protoMessage2PigTuple(msg, m_colSpec.m_msgDesc[i], m_tupleFactory));
            }

            tuple.append(lastEntry.getKey()); // the version
        }
        return tuple;
    }

    private void _initHtable() throws IOException {
        Configuration hbaseConfig = HBaseConfiguration.create(UDFContext.getUDFContext().getJobConf());
        m_htable = new HTable(hbaseConfig, m_table); // careful: would create a
                                                     // new zk connection which
                                                     // is not necessarily
                                                     // released.
        m_tupleFactory=TupleFactory.getInstance();

    }

}
