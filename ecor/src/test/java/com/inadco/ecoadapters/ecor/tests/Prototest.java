package com.inadco.ecoadapters.ecor.tests;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class Prototest {
    
    public static void prototest () throws Exception {
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(delSrc, overwrite, src, dst)
        fs.de
        
    }

}
