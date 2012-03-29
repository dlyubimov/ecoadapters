package com.inadco.ecoadapters.ecor.tests;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RList;
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
        REXP r ;
        
        r= engine.eval("library(ecor)");
        System.out.println(r.toString());
        
        r = engine.eval("stop('R error.')");
        
        r=engine.eval("T");
        System.out.println(r.getType()==REXP.XT_ARRAY_BOOL_INT);
        
        r=engine.eval("'abc'");
        System.out.println(""+r.getType()+ (r.getType()==REXP.XT_STR));
        
        r=engine.eval("list(a=\"B\")");
        System.out.println(""+r.getType()+ (r.getType()==REXP.XT_VECTOR));
        
        RList rlist = r.asList();
        System.out.println(rlist.at("a").asString());
        
        System.out.println(r.toString());
        
        engine.end();

    }

}
