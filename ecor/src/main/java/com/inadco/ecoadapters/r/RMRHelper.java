package com.inadco.ecoadapters.r;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

public final class RMRHelper {

    public static final String R_ARGS_PROP = "ecor.rargs";

    public static Rengine rTaskSetupHelper(Configuration conf, boolean map) throws IOException {
        // do as much as we can on java side
        // to prevent mixed env call overhead.

        String rhome = System.getenv("R_HOME");
        if (rhome == null)
            throw new IOException("R_HOME is not set in the backend.");

        // find the rjobfile
        String rJobFName = conf.get("ecor.hconffile");
        if (rJobFName == null)
            throw new IOException("ecor.hconffile not set");

        Path[] files = DistributedCache.getLocalCacheFiles(conf);
        Path localJobFile = null;
        for (Path file : files)
            if (file.getName().equals(rJobFName)) {
                localJobFile = file;
                break;
            }

        if (localJobFile == null)
            throw new IOException("Unable to find R hconffile in the distributed cache.");

        String str = conf.get(R_ARGS_PROP);
        String[] args = str == null ? new String[] { "--vanilla" } : str.split(" ");

        Rengine engine = new Rengine(args, false, null);
        boolean ok = false;
        try {

            engine.eval("library(ecor)");
            REXP jconf = engine.createRJavaRef(conf);
            engine.assign("hjconf", jconf);
            engine.assign("hconffile", localJobFile.toString());

            /*
             * apparently this needs to be executed before we set any functions
             * up.
             */
            engine.eval("options(error=quote(dump.frames(\"errframes\", F)))");

            /*
             * some of our callback functions are hidden on purpose in order not
             * to export them into public api
             */
            engine.eval("{f<-getAnywhere('.ecor.tasksetup'); fsetup<<- f$obj[[which(f$where=='namespace:ecor')]]}");
            if (map)
                engine.eval("{f<-getAnywhere('.ecor.maptask'); fmap<<- f$obj[[which(f$where=='namespace:ecor')]]}");
            else
                engine
                    .eval("{f<-getAnywhere('.ecor.reducetask'); freduce<<- f$obj[[which(f$where=='namespace:ecor')]]}");

            REXP r =
                map ? engine.eval("fsetup(hjconf, hconffile, mapsetup=T )") : engine
                    .eval("fsetup(hjconf, hconffile, mapsetup=F )");

            if (r == null)
                throw new IOException("failed to communicate task setup properly, no result returned.");

            switch (r.getType()) {
            case REXP.XT_STR:
            case REXP.XT_ARRAY_STR:
                throw new IOException(String.format("R tasksetup execution exception:%s", r.asString()));
            case REXP.XT_BOOL:
            case REXP.XT_ARRAY_BOOL_INT:
                if (r.asBool().isFALSE())
                    throw new IOException("R task setup was not successful but error code is not available.");
                break;
            default:
                throw new IOException(
                    String.format("failed to communicate task setup properly, unsupported type %d returned.",
                                  r.getType()));
            }
            ok = true;
        } finally {
            if (!ok)
                engine.end();
        }

        return engine;
    }

    public static void addFileToCache(Configuration conf, String[] localFNames, String hdfsTempDir, boolean toCP)
        throws IOException {
        // make sure remote hdfs temp dir exists
        FileSystem fs = FileSystem.get(conf);
        Path dfsTemp = new Path(hdfsTempDir);
        if (!fs.exists(dfsTemp) && !fs.mkdirs(dfsTemp))
            throw new IOException(String.format("Unable to create path %s.", dfsTemp.toString()));

        for (String localFName : localFNames) {
            Path localFPath = new Path(localFName);
            Path remoteFPath = new Path(dfsTemp, localFPath.getName());

            fs.copyFromLocalFile(!toCP, true, localFPath, remoteFPath);

            if (toCP) {
                DistributedCache.addFileToClassPath(remoteFPath, conf);
            } else {
                DistributedCache.addCacheFile(remoteFPath.toUri(), conf);
            }
        }
    }

    // should cover most common writables conversion to R primitive types
    public static void assignWritable(Rengine engine, String varName, Writable w) throws IOException {

        if (w instanceof IntWritable)
            engine.assign(varName, new int[] { ((IntWritable) w).get() });
        else if (w instanceof Text)
            engine.assign(varName, ((Text) w).toString());
        else if (w instanceof BytesWritable) {
            byte[] b = ((BytesWritable) w).getBytes();
            int len = ((BytesWritable) w).getLength();
            if (len < b.length)
                b = Arrays.copyOf(b, len);
            engine.assign("bval____", engine.createRJavaRef(b));
            engine.eval(varName + "<- .jevalArray(bval____);rm(bval____)");

        } else if (w instanceof LongWritable)
            engine.assign(varName, new double[] { ((LongWritable) w).get() });
        else if (w instanceof DoubleWritable)
            engine.assign(varName, new double[] { ((DoubleWritable) w).get() });
        else if (w == null)
            engine.eval(varName + "<-NULL");
        else
            engine.assign(varName, engine.createRJavaRef(w));

    }

}
