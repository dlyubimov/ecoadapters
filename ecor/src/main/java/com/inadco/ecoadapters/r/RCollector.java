package com.inadco.ecoadapters.r;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * R Collector. <P>
 * 
 * For now very simple (String,byte[]) collector 
 * @author dmitriy
 *
 */
public class RCollector {
    
    private final List<String> keys = new ArrayList<String>();
    private final List<byte[]> values = new ArrayList<byte[]>();
    
    private final Text tw = new Text();
    private final BytesWritable bw = new BytesWritable();
    
    public void reset () { 
        keys.clear();
        values.clear();
    }
    
    public void add(String key, byte[] value ){ 
        keys.add(key);
        values.add(value);
    }
    
    @SuppressWarnings({"rawtypes","unchecked"})
    public void outputToContext ( TaskInputOutputContext ctx ) throws InterruptedException, IOException {
        for ( int i = 0; i < keys.size(); i++ ) { 
            tw.set(keys.get(i));
            byte[] v = values.get(i);
            bw.set(v,0,v.length);
            ctx.write(tw, bw);
        }
    }

}
