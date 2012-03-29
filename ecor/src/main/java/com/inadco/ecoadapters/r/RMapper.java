package com.inadco.ecoadapters.r;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RList;
import org.rosuda.JRI.Rengine;

public class RMapper extends Mapper<Writable, Writable, Writable, Writable> {

    private Rengine            engine;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // do as much as we can on java side
        // to prevent mixed env call overhead.
        Configuration conf = context.getConfiguration();
        engine = RMRHelper.rTaskSetupHelper(conf, true);

    }

    @Override
    protected void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {

        REXP jkey = engine.createRJavaRef(key);
        REXP jvalue = engine.createRJavaRef(value);
        engine.assign("hjkey", jkey);
        engine.assign("hjvalue", jvalue);
        REXP r = engine.eval("fmap(hjconf,hjkey, hjvalue)");

        if (r == null)
            throw new IOException("failed to communicate map task properly, no result returned.");

        switch (r.getType()) {
        case REXP.XT_STR:
        case REXP.XT_ARRAY_STR:
            throw new IOException(String.format("R map execution exception:%s", r.asString()));
        case REXP.XT_LIST:
        case REXP.XT_VECTOR:
            break;

        default:
            throw new IOException(
                String.format("failed to communicate task setup properly, unsupported type %d returned.", r.getType()));
        }

        RList collector = r.asList();

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
            if (engine != null)
                engine.end();
        } finally {
            engine = null;
        }
    }


}
