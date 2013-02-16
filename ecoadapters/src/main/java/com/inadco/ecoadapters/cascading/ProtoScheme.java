/**
 *
 *  Copyright © 2010, 2011 Agilone, Inc. All rights reserved.
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
package com.inadco.ecoadapters.cascading;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.inadco.ecoadapters.EcoUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * Cascading Protobuf Scheme for reading/writing sequence files with protobuf
 * payload. <P/>
 *
 * Since Cascading doesn't really support notion of nested messages schema,
 * we only roll-out (or roll-in) the first layer of the tuple only, unlike for
 * Pig and Hive. <P>
 *
 * Repeated fields are converted to an array of value types where applicable
 * (e.g. repeated fixed32 -> <code>int[]</code>); repeated messages are converted to
 * {@link Message}<code>[]</code>. <P/>
 *
 * Nested messages are left intact (i.e. go just as is as {@link Message} type).
 *
 * @author Dmitriy
 */
public final class ProtoScheme extends SequenceFile {

    private Descriptors.Descriptor m_msgDesc;
    private CascadingTupleMap m_tupleMap;
    private Text m_outKey = new Text();

    /**
     * Same message descriptor as with pig and hive etc. adapters.
     * I.e. either class name or hdfs reference to the descirptor file + ?msg="message".
     * See  e.g. {@link com.inadco.ecoadapters.pig.SequenceFileProtobufLoader}, or README,
     * for details.<P/>
     *
     * @param msgDescString
     * @throws IOException
     */
    public ProtoScheme(String msgDescString) throws IOException {
        super();
        try {
            if (msgDescString.startsWith("hdfs://"))
                m_msgDesc = EcoUtil.inferDescriptorFromFilesystem(msgDescString);
            else
                m_msgDesc = EcoUtil.inferDescriptorFromClassName(msgDescString);
        } catch (IOException e) {
            throw e;
        } catch (Throwable thr) {
            throw new IOException(thr);
        }
        m_tupleMap = new CascadingTupleMap(m_msgDesc);
        String[] fnames = m_tupleMap.getFieldNames();
        setSourceFields(new Fields(fnames));
        setSinkFields(new Fields(fnames));


    }

    public static void main(String[] args) {
        System.out.printf("%d\n", new Fields(new String[]{"a", "B"}).size());
    }

    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) {
        super.sourcePrepare(flowProcess, sourceCall);
        if (!(sourceCall.getContext()[1] instanceof BytesWritable))
            throw new RuntimeException(
                    "Cascading adapter for protobuff sequence files requires BytesWritable as file value type");
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        super.sinkConfInit(flowProcess, tap, conf);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(BytesWritable.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void sinkPrepare(FlowProcess<JobConf> flowProcess, SinkCall<Void, OutputCollector> sinkCall) throws IOException {
        super.sinkPrepare(flowProcess, sinkCall);
        ((SinkCall) sinkCall).setContext(new BytesWritable());
    }

    @Override
    public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        Writable key = (Writable) sourceCall.getContext()[0];
        BytesWritable bw = (BytesWritable) sourceCall.getContext()[1];
        boolean result = sourceCall.getInput().next(key, bw);

        if (!result)
            return false;

        Tuple tuple = sourceCall.getIncomingEntry().getTuple();
        DynamicMessage msg = DynamicMessage.parseFrom(m_msgDesc,
                CodedInputStream.newInstance(bw.getBytes(), 0, bw.getLength()));
        m_tupleMap.proto2T(msg, tuple);
        return true;
    }

    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Void, OutputCollector> sinkCall) throws IOException {
        Tuple t = sinkCall.getOutgoingEntry().getTuple();
        Message.Builder b = m_tupleMap.t2proto(t);
        byte[] msgBytes = b.build().toByteArray();
        @SuppressWarnings({"unchecked", "rawtypes"})
        BytesWritable val = (BytesWritable) ((SinkCall) sinkCall).getContext();
        val.set(msgBytes, 0, msgBytes.length);
        sinkCall.getOutput().collect(m_outKey, val);
    }
}

