package com.inadco.ecoadapters.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * Factory method collection for object inspectors (mainly for {@link Descriptors.Descriptor}, 
 * i.e. protobuf message structures ) <P>
 * 
 * @author lyubimovd
 *
 */
public class ProtobufMessageObjectInspectorFactory   {

    

    /**
     * Create Hive inspector for a protobuf message 
     * 
     * @param desc Protobuf message descriptor 
     * @return standard Hive's structure object inspector 
     */
    public static StructObjectInspector createProtobufObjectInspectorFor ( Descriptors.Descriptor desc ){ 
        List<String> names = new ArrayList<String>();
        List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>();
        
        for ( FieldDescriptor fd:desc.getFields()) { 
            String name = fd.getName();
            names.add(name);
            ObjectInspector inspector=createObjectInspector(fd);
            inspectors.add(inspector);
        }
        
        // error field inspector.
        names.add("___ERROR___");
        inspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        
        return ObjectInspectorFactory.getStandardStructObjectInspector(
                names, 
                inspectors);
    }
    
    private static ObjectInspector createObjectInspector ( FieldDescriptor fd ) { 
        if ( fd.isRepeated()) return createListObjectInspector(fd);
        else return createNonRepeatedObjectInspector(fd);
    }

    private static ObjectInspector createListObjectInspector ( FieldDescriptor fd ) { 
        return ObjectInspectorFactory.getStandardListObjectInspector(createNonRepeatedObjectInspector(fd));
    }
    
    private static ObjectInspector createNonRepeatedObjectInspector ( FieldDescriptor fd ) { 
        switch ( fd.getType() ) {
        case BOOL: return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
        case BYTES: return  PrimitiveObjectInspectorFactory.javaStringObjectInspector; // we will convert binary to hex, i guess
        case DOUBLE:  return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        
        case SINT32:
        case SFIXED32:
        case UINT32:
        case INT32:
        case FIXED32: return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        case SINT64:
        case SFIXED64:
        case INT64:
        case UINT64:
        case FIXED64: return  PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        case FLOAT: return PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
        case MESSAGE: return createProtobufObjectInspectorFor(fd.getMessageType());
        case STRING: return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        
        case ENUM:return PrimitiveObjectInspectorFactory.javaStringObjectInspector; // we'll convert them to name
        
        case GROUP:
        
        default : throw new UnsupportedOperationException();
            
        }
        
    }
    

    
}
