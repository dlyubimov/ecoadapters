package com.inadco.ecoadapters.pig;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.PigException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.inadco.ecoadapters.EcoUtil;

class HBaseColSpec {

    byte[][] m_fams;
    byte[][] m_cols;
    Descriptor[] m_msgDesc;
    Message.Builder[] m_msgBuilder;
    Schema[] m_pigSchema;

    public HBaseColSpec(String colSpecStr, boolean prepPigSchemas)
            throws PigException {
        super();
        try {
            String[] colSpecs = colSpecStr.split("\\s");

            m_fams = new byte[colSpecs.length][];
            m_cols = new byte[colSpecs.length][];
            m_msgDesc = new Descriptor[colSpecs.length];
            m_msgBuilder = new Message.Builder[colSpecs.length];
            if (prepPigSchemas)
                m_pigSchema = new Schema[colSpecs.length];
            for (int i = 0; i < colSpecs.length; i++) {
                String colSpec = colSpecs[i];
                int famPos = colSpec.indexOf(':');
                if (famPos < 0)
                    throw new PigException(
                            "column spec must have both family and column name");
                m_fams[i] = Bytes.toBytes(colSpec.substring(0, famPos));

                int typePos = colSpec.indexOf(':', famPos + 1);

                if (typePos < 0) {
                    m_cols[i] = Bytes.toBytes(colSpec.substring(famPos + 1));
                } else {
                    m_cols[i] = Bytes.toBytes(colSpec.substring(famPos + 1,
                            typePos));
                    String msgDescString = colSpec.substring(typePos + 1);
                    if (msgDescString.startsWith("hdfs://"))
                        m_msgDesc[i] = EcoUtil
                                .inferDescriptorFromFilesystem(msgDescString);
                    else
                        m_msgDesc[i] = EcoUtil
                                .inferDescriptorFromClassName(msgDescString);

                    if (m_msgDesc[i] == null)
                        throw new PigException(
                                String.format(
                                        "Unable to retrieve protobuf message descriptor for message '%s.'",
                                        msgDescString));

                    m_msgBuilder[i] = DynamicMessage.newBuilder(m_msgDesc[i]);

                    if (prepPigSchemas)
                        m_pigSchema[i] = PigUtil
                                .generatePigSchemaFromProto(m_msgDesc[i]);

                    // if (LOG.isDebugEnabled())
                    // LOG.debug(String.format("Loaded LoadFunc for message class:%s",
                    // msgDescString));

                }

            }

        } catch (PigException exc) {
            throw exc;
        } catch (Throwable exc) {
            throw new PigException(exc);
        }
    }

}
