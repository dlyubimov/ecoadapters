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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * Hive inspector factory for protobuf messages.
 * <P>
 * 
 * @author Dmitriy
 * 
 */
class ProtoInspectorFactory {

    /**
     * Create Hive inspector for a protobuf message
     * 
     * @param desc
     *            Protobuf message descriptor
     * @return Hive structure object inspector
     */
    public static StructObjectInspector createProtobufInspector(
            Descriptors.Descriptor desc) {
        List<String> names = new ArrayList<String>();
        List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>();

        for (FieldDescriptor fd : desc.getFields()) {
            String name = fd.getName();
            names.add(name);
            ObjectInspector inspector = createInspector(fd);
            inspectors.add(inspector);
        }

        names.add("___ERROR___");
        inspectors
                .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(names,
                inspectors);
    }

    private static ObjectInspector createInspector(FieldDescriptor fd) {
        if (fd.isRepeated())
            return createListInspector(fd);
        else
            return createNonRepeatedInspector(fd);
    }

    private static ObjectInspector createNonRepeatedInspector(
            FieldDescriptor fd) {
        switch (fd.getType()) {
        case BOOL:
            return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
        case BYTES:
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        case DOUBLE:
            return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;

        case SINT32:
        case SFIXED32:
        case UINT32:
        case INT32:
        case FIXED32:
            return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        case SINT64:
        case SFIXED64:
        case INT64:
        case UINT64:
        case FIXED64:
            return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        case FLOAT:
            return PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
        case MESSAGE:
            return createProtobufInspector(fd.getMessageType());
        case STRING:
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        case ENUM:
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        case GROUP:

        default:
            throw new UnsupportedOperationException();

        }
    }

    private static ObjectInspector createListInspector(FieldDescriptor fd) {
        return ObjectInspectorFactory
                .getStandardListObjectInspector(createNonRepeatedInspector(fd));
    }

}
