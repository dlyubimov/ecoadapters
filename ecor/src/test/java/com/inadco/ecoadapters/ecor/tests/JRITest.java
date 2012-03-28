package com.inadco.ecoadapters.ecor.tests;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.rosuda.JRI.Rengine;
import org.testng.annotations.Test;

public class JRITest {
    
    public void scratchpad()  {
        Job j=null;
        j.setJobName("a");
        Configuration conf = null; 
        
    }
    
    @Test
    public static void libpathTest () throws Exception { 
        System.out.println(System.getProperty("java.library.path"));
    }
    
    @Test
    public static void jriTest() throws Exception {
        String[] args = new String[] { "--vanilla" };
        
        Rengine engine = new Rengine(args, false, null);

        engine.eval("library(ecor)");

    }

}
