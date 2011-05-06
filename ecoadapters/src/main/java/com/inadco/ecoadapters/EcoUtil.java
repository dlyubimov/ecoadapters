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
package com.inadco.ecoadapters;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;

/**
 * utils shared by hive and pig adapters, mainly protobuf-centric
 * 
 * @author dmitriy
 * 
 */
public final class EcoUtil {

    private final static String NET_ENCODING = "utf-8";

    public static Descriptor inferDescriptorFromClassName(String msgClsName) throws Throwable {
        if (msgClsName == null)
            throw new RuntimeException("messageClass option missing");
        Class<? extends Message> cls = Class.forName(msgClsName).asSubclass(Message.class);
        // so there should be generated static method newBuilder that would
        // return
        // Message.Builder
        Method newBldMtd = cls.getMethod("newBuilder");
        try {
            Message.Builder msgBuilder = (Message.Builder) newBldMtd.invoke(null);
            return msgBuilder.getDescriptorForType();
        } catch (InvocationTargetException exc) {
            throw exc.getTargetException();
        }
    }

    /**
     * quick and dirty implementation to infer message descriptor from the url
     * given. e.g.
     * hdfs://nameserver:11010/data/inadco/proto/testmessages.protodesc
     * ?msg="TestLogMsg"
     * 
     * @param descriptorUrl
     * @return {@link Descriptor} of the message requested, or null if none
     *         found
     */

    public static Descriptor inferDescriptorFromFilesystem(String descriptorUrl) throws IOException,
        URISyntaxException, DescriptorValidationException {
        URI uri = new URI(descriptorUrl);
        String query;
        String msgName = parseQuery((query = uri.getQuery())).getProperty("msg");
        if (msgName == null)
            throw new IOException("need to know msg name!");
        String pathStr =
            query.length() > 0 ? descriptorUrl.substring(0, descriptorUrl.length() - query.length() - 1)
                : descriptorUrl;

        Map<String, Descriptor> msgMap = inferDescriptorsFromFilesystem(pathStr);
        return inferDescWithNestedTypes(msgMap, msgName);
    }

    /**
     * the part before ? is in spring resource notation
     * 
     * @param descriptorUrl
     *            the part before ? is in spring resource notation, msg param
     *            contains the msg name
     * @return descriptor
     */
    public static Descriptor inferDescriptorFromSpringResource(String descriptorUrl) throws IOException,
        URISyntaxException, DescriptorValidationException {
        int qPos = descriptorUrl.indexOf('?');

        String resourceSpec = qPos < 0 ? descriptorUrl : descriptorUrl.substring(0, qPos);
        String querySpec = qPos < 0 ? "" : descriptorUrl.substring(qPos + 1);

        String msgName = parseQuery(querySpec).getProperty("msg");
        if (msgName == null)
            throw new IOException("need to know msg name!");

        Resource springResource = new DefaultResourceLoader().getResource(resourceSpec);
        InputStream is = springResource.getInputStream();
        try {
            Map<String, Descriptor> msgMap = inferDescriptorsFromStream(is);
            return inferDescWithNestedTypes(msgMap, msgName);
        } finally {
            is.close();
        }
    }

    public static Map<String, FileDescriptor> inferFileDescriptorsFromFilesystem(String descriptorUrl)
        throws IOException, URISyntaxException, DescriptorValidationException {
        return buildFileDescriptors(loadDFS(descriptorUrl));
    }

    public static Map<String, FileDescriptor> inferFileDescriptorsFromStream(InputStream descriptorStream)
        throws IOException, URISyntaxException, DescriptorValidationException {
        return buildFileDescriptors(loadFromStream(descriptorStream));
    }

    public static Map<String, Descriptor> inferDescriptorsFromFilesystem(String descriptorUrl) throws IOException,
        URISyntaxException, DescriptorValidationException {
        return buildMessageDescriptors(inferFileDescriptorsFromFilesystem(descriptorUrl).values());
    }

    public static Map<String, Descriptor> inferDescriptorsFromStream(InputStream descriptorStream) throws IOException,
        URISyntaxException, DescriptorValidationException {
        return buildMessageDescriptors(inferFileDescriptorsFromStream(descriptorStream).values());
    }

    private static Descriptor inferDescWithNestedTypes(Map<String, Descriptor> topTypes, String msgName) {
        String[] nesting = msgName.split("\\$");
        Descriptor topDesc;
        int i;
        for (i = 1, topDesc = topTypes.get(nesting[0]); i < nesting.length && topDesc != null; topDesc =
            topDesc.findNestedTypeByName(nesting[i++]))
            ;
        return topDesc;
    }

    private static FileDescriptorSet loadDFS(String descriptorUrl) throws URISyntaxException, IOException {

        Configuration conf = new Configuration();
        FileDescriptorSet fds;
        FileSystem fs = FileSystem.get(new URI(descriptorUrl), conf);
        try {
            Path path = new Path(descriptorUrl);
            if (!fs.isFile(path))
                throw new IOException(String.format("Can't find file '%s'", path.getName()));

            FSDataInputStream fdis = fs.open(path);
            try {
                fds = FileDescriptorSet.parseFrom(fdis);
            } finally {
                fdis.close();
            }

        } finally {
            // fs.close();// is it a singleton though?
        }
        return fds;
    }

    private static FileDescriptorSet loadFromStream(InputStream is) throws IOException {
        return FileDescriptorSet.parseFrom(is);
    }

    private static Map<String, FileDescriptor> buildFileDescriptors(FileDescriptorSet fds)
        throws DescriptorValidationException, IOException {
        Map<String, FileDescriptorProto> files = new HashMap<String, FileDescriptorProto>();
        for (FileDescriptorProto fileDesc : fds.getFileList())
            files.put(fileDesc.getName(), fileDesc);

        Map<String, FileDescriptor> fileDescriptors = new HashMap<String, FileDescriptor>();
        Set<String> limbo = new HashSet<String>();

        while (!files.isEmpty())
            resolve(fileDescriptors, files, limbo, files.keySet().iterator().next());
        return fileDescriptors;

    }

    private static Map<String, Descriptor> buildMessageDescriptors(Collection<? extends FileDescriptor> fileDescriptors) {

        Map<String, Descriptor> descriptors = new HashMap<String, Descriptor>();
        for (FileDescriptor fd : fileDescriptors) {

            for (Descriptor md : fd.getMessageTypes())
                descriptors.put(md.getFullName(), md);
        }
        return descriptors;

    }

    // process DAG of proto descriptor files, detect loops
    private static void resolve(Map<String, FileDescriptor> processed,
                                Map<String, FileDescriptorProto> unprocessed,
                                Set<String> limbo,
                                String toProcess) throws IOException, DescriptorValidationException {

        if (limbo.contains(toProcess))
            throw new IOException(String.format("Circular dependency found on file '%s'. ", toProcess));

        FileDescriptorProto nextFileDesc = unprocessed.remove(toProcess);
        if (nextFileDesc == null)
            throw new IOException(String.format("Unable to find referenced file or circular dependency: '%s' .",
                                                toProcess));
        limbo.add(toProcess);

        // make sure all imports are resolved first
        FileDescriptor[] deps = new FileDescriptor[nextFileDesc.getDependencyCount()];
        int i = 0;
        for (String dependency : nextFileDesc.getDependencyList()) {
            if (!processed.containsKey(dependency))
                resolve(processed, unprocessed, limbo, dependency);
            deps[i++] = processed.get(dependency);
        }
        processed.put(toProcess, FileDescriptor.buildFrom(nextFileDesc, deps));
        limbo.remove(toProcess);
    }

    private static Properties parseQuery(String query) throws IOException {
        Properties props = new Properties();

        for (StringTokenizer st = new StringTokenizer(query, "&"); st.hasMoreTokens();) {
            String paramStr = st.nextToken().trim();
            if (paramStr.length() == 0)
                continue;

            int pos = paramStr.indexOf('=');
            String paramName = pos >= 0 ? paramStr.substring(0, pos) : paramStr;
            paramName = URLDecoder.decode(paramName, NET_ENCODING);
            String paramValue = pos >= 0 ? URLDecoder.decode(paramStr.substring(pos + 1), NET_ENCODING) : null;
            props.put(paramName, paramValue);
        }
        return props;

    }

    /**
     * compare 2 byte strings using unsigned byte ordering
     * 
     * @param bs1
     * @param bs2
     * @return
     */
    public static int compareByteStrings(ByteString bs1, ByteString bs2) {
        int l1 = bs1.size(), l2 = bs2.size();
        int len = Math.min(l1, l2);
        for (int i = 0; i < len; i++) {
            int b1 = bs1.byteAt(i) & 0xff; // unsigned comparison
            int b2 = bs2.byteAt(i) & 0xff;
            if (b1 != b2)
                return b1 - b2;
        }
        if (l1 == l2)
            return 0;
        else if (l1 < l2)
            return -1;
        else
            return 1;
    }

}
