package com.inadco.ecoadapters.r;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

public class RReducer extends Reducer<Text, BytesWritable, Writable, Writable> {

    private Rengine    engine;
    private RCollector collector = new RCollector();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // do as much as we can on java side
        // to prevent mixed env call overhead.
        Configuration conf = context.getConfiguration();
        engine = RMRHelper.rTaskSetupHelper(conf, false);
        REXP rcollector = engine.createRJavaRef(collector);
        engine.assign("ecocollector____", rcollector);

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

    @Override
    protected void reduce(Text keyIn, Iterable<BytesWritable> values, Context ctx) throws IOException,
        InterruptedException {
        engine.assign("hkey", keyIn.toString());
        engine.assign("hjvaliter", engine.createRJavaRef(values));

        REXP r = engine.eval("freduce(hjconf, hkey, hjvaliter)");

        if (r == null)
            throw new IOException("failed to communicate to reduce task properly, no result returned.");

        switch (r.getType()) {
        case REXP.XT_STR:
        case REXP.XT_ARRAY_STR:
            throw new IOException(String.format("R reduce task execution exception:%s", r.asString()));
        case REXP.XT_BOOL:
        case REXP.XT_ARRAY_BOOL_INT:
            if (r.asBool().isFALSE())
                throw new IOException("R reduce task was not successful but error code is not available.");
            break;
        default:
            throw new IOException(
                String.format("failed to communicate to reduce task properly, unsupported type %d returned.", r.getType()));
        }
        try {
            collector.outputToContext(ctx);
        } finally {
            collector.reset();
        }

    }

}
