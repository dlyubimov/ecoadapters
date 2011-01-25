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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * Various pig2proto and vice versa utils.
 * 
 * @author dmitriy
 *
 */
public class PigUtil {

    public static final String ERROR_ATTR = "ERROR___"; 
    
    static public Object protoAny2Pig ( Object src, FieldDescriptor fd, TupleFactory tf ) {
        
        switch ( fd.getType()) { 
        case MESSAGE:
            Message msgField=(Message)src;
            return protoMessage2PigTuple(msgField, fd.getMessageType(),tf);
        case BYTES: 
            return new DataByteArray(((ByteString) src).toByteArray());
            
        case BOOL: 
            // -- see note in schema gen: we translate 
            // protobuf booleans into pig's integers.
            if ( src instanceof Boolean ) 
                return (Boolean)src?1:0;
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
            EnumValueDescriptor enDesc=(EnumValueDescriptor) src;
            return enDesc.getName();
            
        case GROUP:
        
        default : throw new UnsupportedOperationException();            
            
        }
    }

    static public Schema generatePigSchemaFromProto(Descriptor d) throws PigException {
    	Schema sch = new Schema();
    	Schema.setSchemaDefaultType(sch, DataType.TUPLE);
    
    	for (FieldDescriptor fd : d.getFields()) {
    		if (fd.isRepeated())
    			sch.add(inferRepeatedFieldSchema(fd));
    		else
    			sch.add(inferNonrepeatedFieldSchema(fd));
    	}
    
    	// error field inspector.
    	sch.add(new FieldSchema(ERROR_ATTR, DataType.CHARARRAY));
    	
    
    	return sch;
    }
    
    static public Tuple2ProtoMap generatePigTuple2ProtoMap (Schema pigSchema, Descriptor protoDesc ) throws PigException { 
        return new Tuple2ProtoMap(pigSchema, protoDesc);
    }
    

    static public Tuple protoMessage2PigTuple(Message msg, Descriptor desc, TupleFactory tf ) {
    	
    	Tuple msgTuple=tf.newTuple();
    	
        for ( FieldDescriptor fd :desc.getFields()) { 
            if ( fd.isRepeated() )  
                msgTuple.append(parseRepeatedField(msg, fd, tf)) ;
            else { 
                if ( msg.hasField(fd) ) msgTuple.append(parseNonRepeated(msg,fd, tf));
                else msgTuple.append(null); // TBD: is it allowed in pig? don't see why not
            } 
        }
        msgTuple.append(null); // no error
        return msgTuple;
    }

    /**
     * 
     * @param src
     * @param parseMap
     * @param builder
     * @param beginAttribute start with this attribute, inclusive
     * @param endAttribute end with this attribute, exclusive
     * @throws ExecException
     */
    static public void pigTuple2ProtoMessage ( Tuple src, Tuple2ProtoMap parseMap, Message.Builder builder, int beginAttribute, int endAttribute) throws ExecException {
        if ( src == null ) return; // don't set anything
        
        for ( int i = beginAttribute; i < endAttribute; i++ ) { 
            
            FieldDescriptor protoFd=parseMap.m_attrMap.get(i-beginAttribute);
            if ( protoFd == null ) 
                // may be unmapped attributes like ___ERROR___
                continue;
            switch ( src.getType(i) ) {
            case DataType.BAG:
                DataBag bag=(DataBag)src.get(i);
                if ( protoFd.getType()==FieldDescriptor.Type.MESSAGE )
                    for (Tuple tuple:bag) {
                        Tuple2ProtoMap tupleMap=parseMap.m_tupleMap.get(i-beginAttribute);
                        if ( tupleMap==null ) 
                            throw new ExecException ( "Unable to save incoming bag of tuples, no tuple mapping found");
                        Message.Builder tupleBuilder = DynamicMessage.newBuilder(tupleMap.m_protoDesc);
                        pigTuple2ProtoMessage ( tuple,tupleMap,tupleBuilder,0,tuple.size());
                        builder.addRepeatedField( protoFd, tupleBuilder.build());
                    }
                else 
                    for ( Tuple tuple:bag ) {
                        if ( tuple.getAll().size()!=1) 
                            throw new ExecException ( "Bag of a simple type expected, but incoming bag tuple does not have exactly 1 argument.");
                        Object pigObject = tuple.get(0);
                        builder.addRepeatedField(protoFd, parseSimpleProtoType( protoFd, pigObject));
                    }
                break;
            case DataType.TUPLE : 
                if ( protoFd.getType()==FieldDescriptor.Type.MESSAGE ) { 
                    Tuple2ProtoMap tupleMap = parseMap.m_tupleMap.get(i-beginAttribute);
                    if ( tupleMap == null ) 
                        throw new ExecException ( "Unabel to same incoming tuple, no tuple mapping found");
                    Message.Builder tupleBuilder = DynamicMessage.newBuilder(tupleMap.m_protoDesc);
                    Tuple tuple = DataType.toTuple(src.get(i));
                    pigTuple2ProtoMessage ( tuple,tupleMap,tupleBuilder,0,tuple.size());
                    builder.setField( protoFd, tupleBuilder.build());
                } else { 
                    // try to tread as tuple with one attr 
                    Object pigValue=privatePigTupleSingletonToSimpleType(DataType.toTuple(src.get(i)));
                    if ( pigValue == null ) 
                        builder.clearField(protoFd);
                    else 
                        builder.setField ( protoFd, parseSimpleProtoType(protoFd, pigValue));
                }
                break;
                
                // otherwise assume simple value or map. Map is not really supported. 
            default:
                Object protoObj=parseSimpleProtoType ( protoFd, src.get(i));
                if ( protoObj != null ) 
                    builder.setField(protoFd, protoObj);
                    
            }
        }
    }
    
    static public String stringifySchema (Schema pigSchema ) throws IOException { 
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos=new ObjectOutputStream(baos);
        try { 
            oos.writeObject(pigSchema);
        } finally { 
            oos.close();
        }
        return new String(Base64.encodeBase64(baos.toByteArray()), "ASCII");
    }
    
    static public Schema destringifySchema ( String inString ) throws IOException, ClassNotFoundException { 
        ByteArrayInputStream bais=new ByteArrayInputStream(Base64.decodeBase64(inString.getBytes("ASCII")));
        ObjectInputStream ois=new ObjectInputStream(bais);
        try { 
            return (Schema)ois.readObject();
        } finally { 
            ois.close();
        }
    }

    private static FieldSchema inferNonrepeatedFieldSchema(FieldDescriptor fd)
    		throws PigException {
    	switch (fd.getType()) {
    	case BOOL:
    	    
    		// return new FieldSchema(fd.getName(), DataType.BOOLEAN);
    	    // Actually, pig doesn't support boolean semantic correctly in 
    	    // expression comparisons, nor it supports boolean constants (true and false) in 
    	    // semantic analysis. so we convert boolean values to int pig type, i guess.
    	    // in future, we can turn correct boolean semantic conversion on if it is fully supported in pig.
    	    // Right now, boolean type seems to be supported only as a result of boolean/comparison operators 
    	    return new FieldSchema(fd.getName(), DataType.INTEGER);
    	    
    	case BYTES:
    		return new FieldSchema(fd.getName(), DataType.BYTEARRAY); // we will
    		// convert
    		// binary
    		// to
    		// hex,
    		// i
    		// guess
    	case DOUBLE:
    		return new FieldSchema(fd.getName(), DataType.DOUBLE);
    
    	case SINT32:
    	case SFIXED32:
    	case UINT32:
    	case INT32:
    	case FIXED32:
    		return new FieldSchema(fd.getName(), DataType.INTEGER);
    	case SINT64:
    	case SFIXED64:
    	case INT64:
    	case UINT64:
    	case FIXED64:
    		return new FieldSchema(fd.getName(), DataType.LONG);
    	case FLOAT:
    		return new FieldSchema(fd.getName(), DataType.FLOAT);
    	case MESSAGE:
    		return new FieldSchema(fd.getName(), generatePigSchemaFromProto(fd
    				.getMessageType()), DataType.TUPLE);
    	case STRING:
    		return new FieldSchema(fd.getName(), DataType.CHARARRAY);
    
    	case ENUM:
    		return new FieldSchema(fd.getName(), DataType.CHARARRAY); // we'll
    		// convert
    		// them
    		// to
    		// name
    
    	case GROUP:
    
    	default:
    		throw new UnsupportedOperationException();
    
    	}
    
    }

    private static FieldSchema inferRepeatedFieldSchema(FieldDescriptor fd)
    		throws PigException {
//    	return new FieldSchema(fd.getName(), new Schema(
//    			inferNonrepeatedFieldSchema(fd)), DataType.BAG);
        
        FieldSchema bagSchema = new FieldSchema ( fd.getName(), DataType.BAG);
        
        FieldSchema contentSchema=inferNonrepeatedFieldSchema(fd);
        if ( contentSchema.type==DataType.TUPLE && contentSchema.schema!=null ) { // sub-tuple 
//            bagSchema.schema=new Schema(contentSchema);
            bagSchema=contentSchema;
            bagSchema.type=DataType.BAG;

        } else { 
//            FieldSchema  tupleSchema = new FieldSchema(fd.getName(),DataType.TUPLE);
//            tupleSchema.schema=new Schema(contentSchema);
//            bagSchema.schema=new Schema (tupleSchema);
            
//            bagSchema.schema = SchemaUtil.newTupleSchema(fd.getName(), new String[] {fd.getName()}, new Byte[] {contentSchema.type});
            bagSchema.schema=new Schema(contentSchema);
        }
        
        return bagSchema;
        
    }

    static private Object parseNonRepeated ( Message msg, FieldDescriptor fd, TupleFactory tf ) {
        return protoAny2Pig(msg.getField(fd),fd, tf);
    }

    static private DataBag parseRepeatedField ( Message msg, FieldDescriptor fd, TupleFactory tf ) {
    	DataBag db= new DefaultDataBag(); 
        
        int cnt = msg.getRepeatedFieldCount(fd);
        if ( fd.getType()==FieldDescriptor.Type.MESSAGE) 
        for ( int i =0; i < cnt; i++) 
        	db.add(protoMessage2PigTuple((Message)msg.getRepeatedField(fd, i),fd.getMessageType(),tf));
        else for ( int i = 0; i < cnt; i++ ) 
        	db.add(tf.newTuple(protoAny2Pig(msg.getRepeatedField(fd, i),fd,tf)));
        
        return db;
    }
    
    static private Object privatePigTupleSingletonToSimpleType ( Tuple singleton ) throws ExecException { 
        if ( singleton.getAll().size()!=1 ) 
            throw new ExecException ( "tuple with one attribute expected");
        return singleton.get(0);
    }
    
    static private Object parseSimpleProtoType ( FieldDescriptor fd,  Object pigObject  ) throws ExecException {
        if ( pigObject == null ) return null; 
        
        switch ( fd.getType() ) { 
        case BOOL:
            return DataType.toInteger(pigObject)!=0;
        case BYTES:
            if ( pigObject instanceof DataByteArray ) {
                DataByteArray dba = ( DataByteArray) pigObject;
                ByteString bs = ByteString.copyFrom(dba.get(),0,dba.size());
                return bs;
            } else if ( pigObject instanceof byte[] ) { 
                return ByteString.copyFrom ( (byte[])pigObject );
//            } else if ( pigObject == null ) 
//                return ByteString.EMPTY;
            }else throw new ExecException ( "Unsupported conversion to byte array. ");
        case DOUBLE:
            return DataType.toDouble(pigObject);
        case SINT32:
        case SFIXED32:
        case UINT32:
        case INT32:
        case FIXED32:
            return DataType.toInteger(pigObject);
        case SINT64:
        case SFIXED64:
        case INT64:
        case UINT64:
        case FIXED64:
            return DataType.toLong(pigObject);
        case FLOAT:
            return DataType.toFloat(pigObject);
        case STRING:
            return DataType.toString(pigObject);
        case ENUM:
            return fd.getEnumType().findValueByName(DataType.toString(pigObject));
    
        default:
            throw new ExecException ( "Unsupported proto data type in the protodesc.");
        }
    }
    
    private static String stripPigNamespace ( String attrName ) { 
        if ( attrName==null ) return null;
        int pos=attrName.lastIndexOf("::");
        if ( pos < 0 ) return attrName;
        return attrName.substring(pos+2);
    }

    /**
     * to convert pig tuples to protobuf stuff 
     * 
     * @author dmitriy
     *
     */
    static public final class Tuple2ProtoMap {
        
//        private Schema                      m_pigSchema;
        private Descriptor                  m_protoDesc;
        private Map<Integer, FieldDescriptor> m_attrMap = new HashMap<Integer, FieldDescriptor>();
        private Map<Integer, Tuple2ProtoMap>  m_tupleMap = new HashMap<Integer, Tuple2ProtoMap>(); // additional mappings for fields that happen to be (repeated) tuples.
        
        private Tuple2ProtoMap(Schema pigSchema, Descriptor desc) throws FrontendException {
            super();
            
//            m_pigSchema = pigSchema;
            m_protoDesc=desc;
            
            for ( int i = 0; i < pigSchema.size() ;i++) { 
                FieldSchema fs=pigSchema.getField(i);
                FieldDescriptor fd=null;
                String falias=stripPigNamespace(fs.alias);
                if ( ERROR_ATTR.equals(falias)) continue; // skip the generated stuff.
                if( fs.canonicalName!= null ) fd=m_protoDesc.findFieldByName(fs.canonicalName);
                if ( fd==null && falias!= null ) fd=m_protoDesc.findFieldByName(falias);
                
                
                if ( fd==null ) 
                    throw new FrontendException(
                        String.format ( "Unable to save pig tuple field '%s(%s)' into protobuf: no such protobuf field",
                                fs.alias, fs.canonicalName));
                m_attrMap.put(i, fd);
                
                byte st = fs.type;
                
                if ( st == DataType.MAP ) 
                    throw new FrontendException ( String.format(
                            "Pig maps are not yet supported in field %s", fs.canonicalName));
                
                
                if ( st == DataType.BAG) {
                    if ( ! fd.isRepeated() )
                        throw new FrontendException(String.format ( 
                                "Unable to save pig field '%s': it's a bag type, so repeated field expected.",fs.canonicalName));
                    //should have subschema, whcih may mean we have a tuple or we may have a simple type wrapped into one-item schema.
                    
                }
                switch ( fd.getType()) { 
                case MESSAGE:
                    
                    Schema containedSchema;
                    // so, with bag, the containment is always a tuple 
                    if ( st == DataType.BAG && fs.schema!=null ) {
                        if ( fs.schema==null||fs.schema.size()!=1 ) 
                            throw new FrontendException ( 
                                    "exactly one field is expected inside a bag");
                            containedSchema=fs.schema.getField(0).schema;
                           
                    } else containedSchema = fs.schema;
                        
                    
                    if ( st == DataType.BAG || st == DataType.TUPLE )
                        m_tupleMap.put(i, new Tuple2ProtoMap(containedSchema,fd.getMessageType()));
                    else throw new FrontendException ( String.format (
                            "Unable to save pig field '(%s:%s)': expected tuple or a bag of tuples.", 
                            fs.alias,fs.canonicalName
                            ));
                    break;
                
                default: 
                    
                }
            }
        }
        
    }
    
    ///////////////////
    // certain raw types conversion strategies for hbase. 
    public static interface Pig2HBaseStrategy { 
        byte[] toHbase(Object pigVal) throws IOException ;  
    }
    
    public static class PigByteArray2HBaseConversion implements Pig2HBaseStrategy {

        @Override
        public byte[] toHbase(Object pigVal) {
            DataByteArray dba=(DataByteArray)pigVal;
            if (dba==null) return null;
            return dba.get();
        }
    }
    
    public static class PigCharArray2HBaseConversion implements Pig2HBaseStrategy {

        @Override
        public byte[] toHbase(Object pigVal) throws IOException {
            String s  = ( String)pigVal;
            if ( s == null ) return null; 
            return s.getBytes("utf-8");
        }

    }
    
    public static class PigTuple2HBaseConversion implements Pig2HBaseStrategy {

        private Tuple2ProtoMap m_protoMap;
        private Message.Builder m_builder;
        
        
        public PigTuple2HBaseConversion(Tuple2ProtoMap protomap, Message.Builder builder) {
            super();
            m_protoMap=protomap;
            m_builder = builder;
        }

        @Override
        public byte[] toHbase(Object pigVal) throws IOException {
            Tuple t = (Tuple) pigVal; 
            if ( t==null ) return null; 
            Message.Builder b;
            PigUtil.pigTuple2ProtoMessage(t, m_protoMap, b=m_builder.clone(), 0, t.size());
            return b.build().toByteArray();
        }
        
        
    }

}
