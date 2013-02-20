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

import cascading.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author  dmitriy
 */
final class CascadingTupleMap {

    private Message.Builder b;
    private List<Descriptors.FieldDescriptor> tupleMaps = new ArrayList<Descriptors.FieldDescriptor>();
    private Map<String, Integer> protoName2tMap = new HashMap<String, Integer>();

    public CascadingTupleMap(Descriptors.Descriptor msgDesc) {
        List<Descriptors.FieldDescriptor> fds = msgDesc.getFields();
        b = DynamicMessage.newBuilder(msgDesc);

        int i = 0;
        for (Descriptors.FieldDescriptor fd : fds) {
            tupleMaps.add(fd);
            protoName2tMap.put(fd.getName(), i);
            i++;
        }
    }

    public String[] getFieldNames() {
        String[] fnames = new String[tupleMaps.size()];
        int i = 0;
        for (Descriptors.FieldDescriptor fd : tupleMaps) {
            fnames[i] = fd.getName();
            i++;
        }
        return fnames;
    }

    public Message.Builder t2proto(Tuple t) throws IOException {
        if (t == null) return null;
        Message.Builder b = this.b.clone();
        int i = 0;
        for (Descriptors.FieldDescriptor fd : tupleMaps) {
            if (fd.isRepeated())
                repeatedC2P(b, fd, t, i);
            else {
                Object o = simpleC2P(fd, t, i);
                if (o != null)
                    b.setField(fd, o);
            }

            i++;
        }
        return b;
    }

    public Tuple proto2T(Message msg, Tuple holder) throws IOException {
        if (msg == null) return null;
        // efficiency killer, i guess..
        // oh well, it started with the protobuf anyway

        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : msg.getAllFields().entrySet()) {
            Descriptors.FieldDescriptor fd = entry.getKey();
            Object val = entry.getValue();
            int ind = protoName2tMap.get(fd.getName());
            if (!fd.isRepeated()) {
                simpleP2C(holder, ind, fd, val);
            } else {
                List<?> lval = (List<?>) val;
                repeatedP2C(holder, ind, fd, lval);
            }
        }
        return holder;
    }

    static private void repeatedP2C(Tuple tupleTo, int ind, Descriptors.FieldDescriptor fd, List<?> lval) throws IOException {
        int i;
        switch (fd.getType()) {
            case BOOL:
                boolean[] barr = new boolean[lval.size()];
                i = 0;
                for (Object o : lval) {
                    barr[i] = (Boolean) o;
                    i++;
                }
                tupleTo.set(ind, barr);
                break;
            case BYTES:
                byte[][] byarr = new byte[lval.size()][];
                i = 0;
                for (Object o : lval) {
                    byarr[i] = (byte[]) o;
                    i++;
                }
                tupleTo.set(ind, byarr);
                break;
            case DOUBLE:
                double[] darr = new double[lval.size()];
                i = 0;
                for (Object o : lval) {
                    darr[i] = (Double) o;
                    i++;
                }
                tupleTo.set(ind, darr);
                break;
            case SINT32:
            case SFIXED32:
            case UINT32:
            case INT32:
            case FIXED32:
                int[] iarr = new int[lval.size()];
                i = 0;
                for (Object o : lval) {
                    iarr[i] = (Integer) o;
                    i++;
                }
                tupleTo.set(ind, iarr);
                break;

            case SINT64:
            case SFIXED64:
            case INT64:
            case UINT64:
            case FIXED64:
                long[] larr = new long[lval.size()];
                i = 0;
                for (Object o : lval) {
                    larr[i] = (Long) o;
                    i++;
                }
                tupleTo.set(ind, larr);

                break;

            case FLOAT:
                float[] farr = new float[lval.size()];
                i = 0;
                for (Object o : lval) {
                    farr[i] = (Float) o;
                    i++;
                }
                tupleTo.set(ind, farr);

                break;
            case STRING:
                String[] sarr = new String[lval.size()];
                i = 0;
                for (Object o : lval) {
                    sarr[i] = (String) o;
                    i++;
                }
                tupleTo.set(ind, sarr);

                break;
            case ENUM:
                sarr = new String[lval.size()];
                i = 0;
                for (Object o : lval) {
                    sarr[i] = ((Descriptors.EnumValueDescriptor) o).getName();
                    i++;
                }
                tupleTo.set(ind, sarr);
                break;
            case MESSAGE:
                Message[] marr = lval.toArray(new Message[lval.size()]);
                tupleTo.set(ind, marr);
                break;

            default:
                throw new IOException("Unsupported proto data type in the protodesc.");
        }

    }

    static private void simpleP2C(Tuple tupleTo, int ind, Descriptors.FieldDescriptor fd, Object val) throws IOException {

        switch (fd.getType()) {
            case BOOL:
                tupleTo.setBoolean(ind, (Boolean) val);
                break;
            case BYTES:
                tupleTo.set(ind, (byte[]) val);
                break;
            case DOUBLE:
                tupleTo.setDouble(ind, (Double) val);
                break;
            case SINT32:
            case SFIXED32:
            case UINT32:
            case INT32:
            case FIXED32:
                tupleTo.setInteger(ind, (Integer) val);
                break;

            case SINT64:
            case SFIXED64:
            case INT64:
            case UINT64:
            case FIXED64:
                tupleTo.setLong(ind, (Long) val);
                break;

            case FLOAT:
                tupleTo.setFloat(ind, (Float) val);
                break;
            case STRING:
                tupleTo.setString(ind, (String) val);
                break;
            case ENUM:
                tupleTo.setString(ind, ((Descriptors.EnumValueDescriptor) val).getName());
                break;
            case MESSAGE:
                tupleTo.set(ind, (Message) val);
                break;

            default:
                throw new IOException("Unsupported proto data type in the protodesc.");
        }

    }

    static private void repeatedC2P(Message.Builder b, Descriptors.FieldDescriptor fd, Tuple t, int ind) throws IOException {
        // expect tuple as a bag of field values
        Object src = t.getObject(ind);
        if (src == null) return;

        switch (fd.getType()) {
            case BOOL:
                for (boolean boo : (boolean[]) src)
                    b.addRepeatedField(fd, boo);
                break;
            case BYTES:
                for (byte[] bbs : (byte[][]) src)
                    b.addRepeatedField(fd, bbs);
                break;
            case DOUBLE:
                for (double boo : (double[]) src)
                    b.addRepeatedField(fd, boo);
                break;
            case SINT32:
            case SFIXED32:
            case UINT32:
            case INT32:
            case FIXED32:
                for (int boo : (int[]) src)
                    b.addRepeatedField(fd, boo);
                break;

            case SINT64:
            case SFIXED64:
            case INT64:
            case UINT64:
            case FIXED64:
                for (long boo : (long[]) src)
                    b.addRepeatedField(fd, boo);
                break;

            case FLOAT:
                for (float boo : (float[]) src)
                    b.addRepeatedField(fd, boo);
                break;
            case STRING:
                try {
                	for (String boo : (String[]) src)
                		b.addRepeatedField(fd, boo);
                } catch(Exception e) {
                	b.addRepeatedField(fd, (String)src);
                }
                break;
            case ENUM:
                Descriptors.EnumDescriptor ed = fd.getEnumType();
                for (String boo : (String[]) src)
                    b.addRepeatedField(fd, ed.findValueByName(boo));
                break;
            case MESSAGE:
                // well, if it is message, we expect it to be Message for cascading case.
                // we do not unfold the maps recursively since cascading
                // doesn't support nested schemas and tuple trees.
                Tuple nestedT = (Tuple) src;
                int sz = nestedT.size();
                for (int i = 0; i < sz; i++) {
                    b.addRepeatedField(fd, (Message) nestedT.getObject(i));
                }
                break;

            default:
                throw new IOException("Unsupported proto data type in the protodesc.");

        }
    }

    static private Object simpleC2P(Descriptors.FieldDescriptor fd, Tuple t, int ind) throws IOException {

        Object obj = null;
        switch (fd.getType()) {
            case BOOL:
                obj = t.getBoolean(ind);
                break;
            case BYTES:
                obj = (byte[]) t.getObject(ind);
                break;
            case DOUBLE:
                obj = t.getDouble(ind);
                break;
            case SINT32:
            case SFIXED32:
            case UINT32:
            case INT32:
            case FIXED32:
                obj = t.getInteger(ind);
                break;

            case SINT64:
            case SFIXED64:
            case INT64:
            case UINT64:
            case FIXED64:
                obj = t.getLong(ind);
                break;

            case FLOAT:
                obj = t.getFloat(ind);
                break;
            case STRING:
                obj = t.getString(ind);
                break;
            case ENUM:
                obj = fd.getEnumType().findValueByName(t.getString(ind));
                break;
            case MESSAGE:
                // well, if it is message, we expect it to be Message for cascading case.
                // we do not unfold the maps recursively since cascading
                // doesn't support nested schemas and tuple trees.
                obj = (Message) t.getObject(ind);
                break;

            default:
                throw new IOException("Unsupported proto data type in the protodesc.");
        }
        return obj;
    }

}
