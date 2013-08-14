package com.inadco.ecoadapters.pig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * A pig StoreFunc to store HBase hFiles
 * 
 * 3 things to keep in mind when using this
 * 
 * 1. you probably want to set 
 * 
 * set hfile.compression 'snappy';
 * 
 * or whichever compression algorithm you're using for the destination table
 * 
 * 2. you probably want to specify
 * 
 * set hbase.mapreduce.hfileoutputformat.compaction.exclude 'true'
 * 
 * to avoid compaction storms, https://issues.apache.org/jira/browse/HBASE-3690
 * 
 * 3. Set the size of the output files using
 * 
 * hbase.hregion.max.filesize
 * 
 * @author michael
 *
 */
@SuppressWarnings("rawtypes")
public class HBaseBulkProtobufStorage extends StoreFunc {


	protected RecordWriter writer = null;
	private Map<String,Integer> familyQualToIndex;
	private String[] qualifiers;
	private String[] families;

	public HBaseBulkProtobufStorage(String storageSpec) {
		String[] storage = storageSpec.split("\\s");

		familyQualToIndex = new HashMap<String,Integer>();
		families = new String[storage.length];
		qualifiers = new String[storage.length];

		TreeSet<KeyValue> map = new TreeSet<KeyValue>(KeyValue.COMPARATOR);

		int i = 0;
		String[] split;

		/* 
		 * For each row, we'll get multiple column families and qualifiers, HFileOutputFormat requires them to arrive
		 * in sorted order, so we sort them once with dummy rowKey and value
		 * 
		 */

		for(String store : storage) {
			split = store.split(":");

			families[i] = split[0];
			qualifiers[i] = split[1];

			KeyValue kv = new KeyValue("row".getBytes(),split[0].getBytes(),split[1].getBytes(),store.getBytes());
			map.add(kv);

			i++;
		}

		i = 0;
		for (KeyValue kv2 : map) {
			familyQualToIndex.put((new String(kv2.getFamily()))+":"+(new String(kv2.getQualifier())), new Integer(i));
			i++;
		}

	}

	public OutputFormat getOutputFormat() throws IOException {
		HFileOutputFormat outputFormat = new HFileOutputFormat();
		return outputFormat;
	}

	public void setStoreLocation(String location, Job job) throws IOException {
		job.getConfiguration().set("mapred.textoutputformat.separator", "");
		FileOutputFormat.setOutputPath(job, new Path(location));
	}

	public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer = writer;
	}

	@SuppressWarnings("unchecked")
	public void putNext(Tuple t) throws IOException {
		try {
			ImmutableBytesWritable hbaseRowKey = new ImmutableBytesWritable(getValue(t.get(0)));
			KeyValue[] arr = new KeyValue[families.length];

			KeyValue kv;
			for(int i = 0;i<qualifiers.length;i++) {
				kv = new KeyValue(getValue(t.get(0)),families[i].getBytes(),qualifiers[i].getBytes(),getValue(t.get(i+1)));
				arr[familyQualToIndex.get((new String(kv.getFamily()))+":"+(new String(kv.getQualifier()))).intValue()] = kv;
			}

			// Ordered write
			for (KeyValue kv2 : arr) {
				writer.write(hbaseRowKey, kv2);
			}
		} catch(Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}

	private byte[] getValue(Object field) throws IOException {
		byte[] value = null; 
		switch (DataType.findType(field)) {
		case DataType.INTEGER: {
			value = Bytes.toBytes(((Integer)field));
			break;
		}
		case DataType.LONG: {
			value = Bytes.toBytes(((Long)field));
			break;
		}

		case DataType.FLOAT: {
			value = Bytes.toBytes(((Float)field));
			break;
		}

		case DataType.DOUBLE: {
			value = Bytes.toBytes(((Double)field));
			break;
		}

		case DataType.CHARARRAY: {
			value = ((String)field).getBytes("UTF-8");
			break;
		}

		case DataType.BYTEARRAY: {
			value = ((DataByteArray)field).get();
			break;
		}

		default: {
			throw new IllegalArgumentException("No type:" + DataType.findType(field));
		}
		}
		return value;
	}
}
