package com.inadco.ecoadapters.r;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

/**
 * This mapper is a simplistic mapper for R-based map reduce jobs. 
 * It can read inputs supporting common input writables for keys or values: 
 * {@link IntWritable}, {@link LongWritable}, {@link Text}, {@link DoubleWritable}
 * and, finally, a {@link BytesWritable}. It converts contents of these writables 
 * on the fly to a suitable primitive R type and passes them to the R map function. <P> 
 *  
 * In case of {@link BytesWritable}, the content is then converted to 'raw' R type,
 * and then R map function has a chance to do whatever it wants with it (e.g. 
 * parse a protobuf message into R object tree, or perhaps treat it as an R serialized 
 * object etc.) <P>
 * 
 * R map function outputs key and value. Key is then coerced to a 1-length character
 * vector and output as {@link Text} for shuffling (or final output). Value R object 
 * is serialized using R serialize() method and saved as a BytesWritable content.<P>
 * 
 */
public class RMapper extends Mapper<Writable, Writable, Text, BytesWritable> {

    private Rengine    engine;
    private RCollector collector = new RCollector();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // do as much as we can on java side
        // to prevent mixed env call overhead.
        Configuration conf = context.getConfiguration();
        engine = RMRHelper.rTaskSetupHelper(conf, true);
        REXP rcollector = engine.createRJavaRef(collector);
        engine.assign("ecocollector____", rcollector);
    }

    @Override
    protected void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {

        RMRHelper.assignWritable(engine, "hjkey", key);
        RMRHelper.assignWritable(engine, "hjvalue", value);

        REXP r = engine.eval("fmap(hjconf,hjkey, hjvalue)");

        if (r == null)
            throw new IOException("failed to communicate to map task properly, no result returned.");

        switch (r.getType()) {
        case REXP.XT_STR:
        case REXP.XT_ARRAY_STR:
            throw new IOException(String.format("R map task execution exception:%s", r.asString()));
        case REXP.XT_BOOL:
        case REXP.XT_ARRAY_BOOL_INT:
            if (r.asBool().isFALSE())
                throw new IOException("R map task was not successful but error code is not available.");
            break;
        default:
            throw new IOException(
                String.format("failed to communicate to map task properly, unsupported type %d returned.", r.getType()));
        }
        try {
            collector.outputToContext(context);
        } finally {
            collector.reset();
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
            if (engine != null)
                engine.end();
        } finally {
            engine = null;
            super.cleanup(context);
        }
    }

}
