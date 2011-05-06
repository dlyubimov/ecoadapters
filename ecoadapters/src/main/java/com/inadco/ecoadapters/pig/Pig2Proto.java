package com.inadco.ecoadapters.pig;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.Descriptor;
import com.inadco.ecoadapters.pig.PigUtil.Tuple2ProtoMap;

/**
 * inverse counterpart of {@link Proto2Pig}.<P>
 * 
 * Does transformation similar to storage funcs such as
 * {@link SequenceFileStorage} by capturing pig attributes and converting them
 * into serializes protobuf message.<P>
 * 
 * Contract for converting is the same as in other storage functions (attribute
 * names in schema must match the names in protobuf message descriptor fields,
 * some type conversion is applied as appropriate).<P>
 * 
 * See {@link Proto2Pig} for usage similarities.<P>
 * 
 * Status: initial development.
 * Problems: incoming pig schema supplied only at front end. we probably can encode it 
 * but multiple invocations for the same proto message will probably be untraceable 
 * unless we use each pig <code>define</code> exactly one time.
 * 
 * @author dmitriy
 * 
 */
public class Pig2Proto extends EvalFunc<byte[]> {
    
    private String                                        m_msgDescString;
    private Schema                                        m_pigSchema;
    private Descriptor                                    m_msgDesc;
    private Message.Builder                               m_msgBuilder;
    private Tuple2ProtoMap                                m_tuple2ProtoMap;


    public Pig2Proto(String msgDescString) {
        super();
        m_msgDescString = msgDescString;
    }

    @Override
    public byte[] exec(Tuple input) throws IOException {
        try {
            if ( input == null ) return null;
            
            Message.Builder bldr = m_msgBuilder.clone();
            PigUtil.pigTuple2ProtoMessage(input, m_tuple2ProtoMap, bldr, 0, input.size());
            byte[] val = bldr.build().toByteArray();
            return val;
        } catch (IOException exc) {
            throw exc;
        } catch (Throwable exc) {
            throw new IOException(exc);
        }
    }

}
