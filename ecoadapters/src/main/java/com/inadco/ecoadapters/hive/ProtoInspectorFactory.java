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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private static Integer maxRecursionDepth = 1;

    /**
     * Create Hive inspector for a protobuf message
     * 
     * @param desc
     *            Protobuf message descriptor
     * @return Hive structure object inspector
     */
    public static StructObjectInspector createProtobufInspector(
            Descriptors.Descriptor desc) {
        Map<String, Integer> seenMap = new HashMap<String, Integer>();
        List<String> names = new ArrayList<String>();
        List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>();

        for (FieldDescriptor fd : desc.getFields()) {
            String name = fd.getName();
            names.add(name);
            ObjectInspector inspector = createInspector(fd, null, "", false, seenMap);
            inspectors.add(inspector);
        }

        names.add("___ERROR___");
        inspectors
                .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(names,
                inspectors);
    }

    /**
     * Create Hive inspector for a protobuf message
     *
     * @param outerDesc
     *            Protobuf message descriptor
     * @param innerDesc
     *            Protobuf message descriptor for a protobuf implicitly contained in outerDesc
     * @param connectingField
     *            The field name that contains the reference to innerDesc
     * @param addError
     *            Add an error field to the struct.
     * @param seenMap
     *            Map of all seen field names, helps avoid recursions in protobuf definitions
     * @return Hive structure object inspector
     */
    public static StructObjectInspector createProtobufInspector(
            Descriptors.Descriptor outerDesc,
            Descriptors.Descriptor innerDesc,
            String connectingField,
            boolean addError,
            Map<String, Integer> seenMap) {
        List<String> names = new ArrayList<String>();
        List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>();

        for (FieldDescriptor fd : outerDesc.getFields()) {
          String name = fd.getName();
          Integer occurances = seenMap.containsKey(name) ? seenMap.get(name) : 0;
          if (occurances < maxRecursionDepth) {

            Integer count = seenMap.containsKey(name) ? seenMap.get(name) : 0;
            seenMap.put(name, count + 1);

            names.add(name);
            ObjectInspector inspector = createInspector(fd, innerDesc, connectingField, addError, seenMap);
            inspectors.add(inspector);

            seenMap.put(name, seenMap.get(name) - 1);
          }
        }

        if (addError) {
            names.add("___ERROR___");
            inspectors
                    .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(names,
                inspectors);
    }

    private static ObjectInspector createInspector(FieldDescriptor fd,
                                                   Descriptors.Descriptor innerDesc,
                                                   String connectingField,
                                                   boolean addError,
                                                   Map<String, Integer> seenMap) {
        if (fd.isRepeated())
            return createListInspector(fd, innerDesc, connectingField, addError, seenMap);
        else
            return createNonRepeatedInspector(fd, innerDesc, connectingField, addError, seenMap);
    }

    private static ObjectInspector createNonRepeatedInspector(
            FieldDescriptor fd,
            Descriptors.Descriptor innerDesc,
            String connectingField,
            boolean addError,
            Map<String, Integer> seenMap) {


        if (innerDesc != null && fd.getName().equals(connectingField)) {
          return createProtobufInspector(innerDesc, null, connectingField, addError, seenMap);
        }

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
            return createProtobufInspector(fd.getMessageType(), innerDesc, connectingField, addError, seenMap);
        case STRING:
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        case ENUM:
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        case GROUP:

        default:
            throw new UnsupportedOperationException();

        }
    }

    private static ObjectInspector createListInspector(FieldDescriptor fd,
                                                       Descriptors.Descriptor innerDesc,
                                                       String connectingField,
                                                       boolean addError,
                                                       Map<String, Integer> seenMap) {
        return ObjectInspectorFactory
                .getStandardListObjectInspector(createNonRepeatedInspector(fd, innerDesc, connectingField, addError, seenMap));
    }

}
