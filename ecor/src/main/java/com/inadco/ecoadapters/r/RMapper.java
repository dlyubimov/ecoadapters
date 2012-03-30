package com.inadco.ecoadapters.r;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

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
