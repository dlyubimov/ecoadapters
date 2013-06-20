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
import java.lang.reflect.Type;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.inadco.ecoadapters.EcoUtil;

/**
 * abstract base to evaluate a serialized protobuf message to pig
 * <P>
 * 
 * Usage: create a pig udf derived from this class and override constructor that
 * supplies the message descriptor.
 * <P>
 * 
 * use
 * 
 * <pre>
 *  define mymsg2Pig com.inadco.ecoadapters.pig.Proto2Pig(mymsg-class-or-uri)
 * </pre>
 * 
 * as you would do with other loader functions (see, for example,
 * {@link SequenceFileProtobufLoader} for description of this urls or
 * classnames).
 * <P>
 * 
 * Status: verification
 * 
 * @author dmitriy
 * 
 */
public class Proto2Pig extends EvalFunc<Tuple> {

    private static final Log         LOG = LogFactory.getLog(Proto2Pig.class);

    protected Descriptor             msgDesc;
    protected DynamicMessage.Builder msgBuilder;
    protected Schema                 pigSchema;
    protected TupleFactory           tupleFactory;

    public Proto2Pig(String msgDescString) {
        super();
        try {
            if (msgDescString.startsWith("hdfs://"))
                msgDesc = EcoUtil.inferDescriptorFromFilesystem(msgDescString);
            else
                msgDesc = EcoUtil.inferDescriptorFromClassName(msgDescString);
            msgBuilder = DynamicMessage.newBuilder(msgDesc);
            pigSchema = PigUtil.generatePigSchemaFromProto(msgDesc);
            tupleFactory = TupleFactory.getInstance();

            if (LOG.isDebugEnabled())
                LOG.debug(String.format("Loaded LoadFunc for message class:%s", msgDescString));

        } catch (Throwable thr) {
            if (thr instanceof RuntimeException)
                throw (RuntimeException) thr;
            else
                throw new RuntimeException(thr);
        }
    }

    @Override
    public Tuple exec(Tuple tuple) throws IOException {
        if (tuple == null)
            return null;
        DataByteArray serMsg = (DataByteArray) tuple.get(0);
        if (serMsg == null ) return null;
        Message msg = msgBuilder.clone().mergeFrom(serMsg.get(), 0, serMsg.size()).buildPartial();
        return PigUtil.protoMessage2PigTuple(msg, msgDesc, tupleFactory);
    }

    @Override
    public Type getReturnType() {
        return Tuple.class;
    }

    @Override
    public Schema outputSchema(Schema input) {

        List<FieldSchema> fields = input.getFields();
        if (fields.size() != 1)
            throw new RuntimeException("Wrong # of arguments in call to ProtoToPig()");
        FieldSchema argSchema = fields.get(0);
        if (argSchema.type != DataType.BYTEARRAY)
            throw new RuntimeException("Wrong argument type in call to ProtoToPig(): expected bytearray.");

        return pigSchema;
    }

}
