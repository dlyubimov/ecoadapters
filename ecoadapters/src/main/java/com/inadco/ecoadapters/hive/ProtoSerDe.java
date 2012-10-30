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
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.inadco.ecoadapters.EcoUtil;

/**
 * {@link SerDe} support for sequence files with values having
 * {@link BytesWritable}-serialized protobuf messages. Only deserialization is
 * supported as of this time.
 * 
 * <pre>
 * CREATE EXTERNAL TABLE IMPRESSION_LOGS 
 * ROW FORMAT SERDE 'com.inadco.ecoadapters.hive.ProtoSerDe'
 * WITH SERDEPROPERTIES ("messageClass"="com.inadco.logging.codegen.test.TestMessages$TestLogProto")
 * STORED AS SEQUENCEFILE
 * LOCATION '/data/inadco/var/log/IMPRESSION'
 * 
 * Found class for com.inadco.ecoadapters.hive.ProtobufSerDe
 * OK
 * Time taken: 3.175 seconds
 * 
 * </pre>
 * 
 * -or -
 * <P>
 * 
 * another way to connect hive to external table is to specify the protodesc
 * file (locally or on hdfs as output by protoc with --descriptor_set_out
 * option) for the files. This way it doesn't require anything but
 * protobuf-java.jar in the $HIVE_HOME/auxlib and doesn't require recompilation
 * of any java code:
 * <P>
 * 
 * <pre>
 * CREATE EXTERNAL TABLE IMPRESSION_LOGS 
 * ROW FORMAT SERDE 'com.inadco.ecoadapters.hive.ProtoSerDe'
 * WITH SERDEPROPERTIES ("fileDescSetUri"="hdfs://localhost:11010/data/inadco/protolib/testMessages.protodesc?msg=inadco.test.TestLogProto")
 * STORED AS SEQUENCEFILE
 * LOCATION '/data/inadco/var/log/IMPRESSION'
 * 
 * </pre>
 * 
 * <pre>
 * hive> describe impression_logs;
 * OK
 * landingpagetitle        string  from deserializer
 * landingpagekeyword      string  from deserializer
 * uniqueurl       string  from deserializer
 * isdelete        boolean from deserializer
 * isnew   boolean from deserializer
 * isdirty boolean from deserializer
 * ___error___ string  from deserializer
 * Time taken: 0.125 seconds
 * 
 * </pre>
 * 
 * each protobuf message also has an implicit column '__serde_error__' which is
 * going to be populated with any deserialization errors that happen during
 * processing. e.g. we can count how many deserialization errors we have in the
 * logs:
 * 
 * <pre>
 * hive> select count(`__serde_error__`), count(1) from impression_logs;
 * Ended Job = job_201004191323_0001
 * OK
 * 0       10000
 * Time taken: 24.724 seconds
 * </pre>
 * <P>
 * 
 * serialization is not supported.
 * 
 * @author dmitriy
 * 
 */
public class ProtoSerDe implements SerDe {

    private static final Logger LOG = Logger.getLogger(ProtoSerDe.class);

    private StructObjectInspector m_protoMsgInspector;
    private Descriptors.Descriptor m_msgDesc;
    private Message.Builder m_msgBuilder;

    public ProtoSerDe() {
        super();
    }

    @Override
    public void initialize(Configuration configuration, Properties props)
            throws SerDeException {

        try {
            String msgClsName = props.getProperty("messageClass");
            String fileDescSetUri = props.getProperty("fileDescSetUri");
            if (msgClsName != null)
                m_msgDesc = EcoUtil.inferDescriptorFromClassName(msgClsName);
            else if (fileDescSetUri != null) {
                m_msgDesc = EcoUtil
                        .inferDescriptorFromFilesystem(fileDescSetUri);
                if (m_msgDesc == null)
                    throw new SerDeException(
                            String.format(
                                    "Unable to locate message in the protodesc file identified by uri %s",
                                    fileDescSetUri));
            } else
                throw new SerDeException(
                        "either FileDescriptorSet's file uri or message class name must be specified.");

            m_msgBuilder = DynamicMessage.newBuilder(m_msgDesc);
            m_protoMsgInspector = ProtoInspectorFactory
                    .createProtobufInspector(m_msgDesc);

            if (LOG.isDebugEnabled())
                LOG.debug(String.format("Loaded SerDe for message '%s'.",
                        m_msgDesc.getName()));

        } catch (SerDeException exc) {
            throw exc;
        } catch (Throwable thr) {
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
            BytesWritable value = (BytesWritable) writableBlob;

            byte[] msgBytes = Arrays
                    .copyOf(value.getBytes(), value.getLength());
            Message msg = m_msgBuilder.clone().mergeFrom(msgBytes)
                    .buildPartial();
            return protoMsg2Hive(msg, m_msgDesc);

        } catch (Throwable thr) {
            return toError(thr);
        }
    }

    @Override
    public Writable serialize(Object src, ObjectInspector oi)
            throws SerDeException {
        throw new SerDeException(
                "serialization into a protobuf object not yet supported");
    }

    private static List<Object> protoMsg2Hive(Message msg,
            Descriptors.Descriptor desc) {
        List<Object> result = new ArrayList<Object>();
        for (FieldDescriptor fd : desc.getFields()) {
            if (fd.isRepeated())
                result.add(repeatedProto2Hive(msg, fd));
            else {
                if (msg.hasField(fd))
                    result.add(nonRepeatedProto2Hive(msg, fd));
                else
                    result.add(null);
            }
        }
        result.add(null);
        return result;
    }

    private static Object repeatedProto2Hive(Message msg, FieldDescriptor fd) {
        List<Object> result = new ArrayList<Object>();
        int cnt = msg.getRepeatedFieldCount(fd);
        for (int i = 0; i < cnt; i++)
            result.add(proto2hive(msg.getRepeatedField(fd, i), fd));
        return result;
    }

    private static Object nonRepeatedProto2Hive(Message msg, FieldDescriptor fd) {
        return proto2hive(msg.getField(fd), fd);
    }

    private static Object proto2hive(Object src, FieldDescriptor fd) {

        switch (fd.getType()) {
        case MESSAGE:
            Message msgField = (Message) src;
            return protoMsg2Hive(msgField, fd.getMessageType());
        case BYTES:
            byte[] val = ((ByteString) src).toByteArray();
            // we convert to string using %X formatting
            return getHexString(val);
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
            EnumValueDescriptor enDesc = (EnumValueDescriptor) src;
            return enDesc.getName();

        case GROUP:

        default:
            throw new UnsupportedOperationException();

        }
    }

    private List<Object> toError(Throwable thr) {

        List<Object> result = new ArrayList<Object>();

        for (int i = 0; i < m_protoMsgInspector.getAllStructFieldRefs().size() - 1; i++)
            result.add(null);

        String error = thr.toString();
        result.add(error);

        return result;

    }

    private static String getHexString(byte[] b) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < b.length; i++) {
           result.append(Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1));
        }
        return result.toString().toUpperCase();
    }

}
