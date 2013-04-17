/**
 * Copyright (c) 2011, The University of Southampton and the individual contributors.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 *   * 	Redistributions of source code must retain the above copyright notice,
 * 	this list of conditions and the following disclaimer.
 *
 *   *	Redistributions in binary form must reproduce the above copyright notice,
 * 	this list of conditions and the following disclaimer in the documentation
 * 	and/or other materials provided with the distribution.
 *
 *   *	Neither the name of the University of Southampton nor the names of its
 * 	contributors may be used to endorse or promote products derived from this
 * 	software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.inadco.ecoadapters.cascading;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

/**
 * Proxy RecordReader that CombineFileRecordReader can instantiate, which itself
 * translates a CombineFileSplit into a FileSplit.
 * 
 * @param <K> Key type 
 * @param <V> Value type
 */
public class CombineSequenceFileRecordReader<K, V> implements RecordReader<K, V> {
	
	private RecordReader<K, V> rr;
	
	public CombineSequenceFileRecordReader(CombineFileSplit split, 
            Configuration conf, 
            Reporter rep,
            Integer index) throws IOException {
		FileSplit filesplit = new FileSplit(split.getPath(index), split.getOffset(index), split.getLength(index), split.getLocations());
        rr = new SequenceFileRecordReader<K, V>(conf, filesplit);
	}

	@Override
	public boolean next(K key, V value) throws IOException {
		return rr.next(key, value);
	}

	@Override
	public K createKey() {
		return rr.createKey();
	}

	@Override
	public V createValue() {
		return rr.createValue();
	}

	@Override
	public long getPos() throws IOException {
		return rr.getPos();
	}

	@Override
	public void close() throws IOException {
		rr.close();
	}

	@Override
	public float getProgress() throws IOException {
		return rr.getProgress();
	}

}