package com.inadco.ecoadapters.r;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

public class RMapper extends Mapper<Writable, Writable, Writable, Writable> {

    public static final String R_ARGS_PROP = "ecor.rargs";

    private Rengine            engine;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration conf = context.getConfiguration();
        String str = conf.get(R_ARGS_PROP);

        String[] args = str == null ? new String[] { "--vanilla" } : str.split(" ");
        
        String rhome = System.getenv("R_HOME");
        if ( rhome == null ) 
            throw new IOException ("R_HOME is not set in the backend.");

        engine = new Rengine(args, false, null);

        engine.eval("library(ecor)");
        REXP jconf = engine.createRJavaRef(conf);
        REXP jcontext = engine.createRJavaRef(context);
        engine.assign("__hjconf", jconf);
        engine.assign("__hjcontext", jcontext);
        engine.eval(".ecor.tasksetup(__hjconf, __hjcontext, mapsetup=T )");
    }
    
    

    @Override
    protected void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
        
        REXP jkey = engine.createRJavaRef(key);
        REXP jvalue = engine.createRJavaRef(value);
        engine.assign("__hjkey",jkey);
        engine.assign("__hjvalue",jvalue);
        engine.eval (".ecor.maptask(__hjconf,__hjcontext,__hjkey, __hjvalue");
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
