package com.inadco.ecoadapters.ecor.tests;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RList;
import org.rosuda.JRI.RVector;
import org.rosuda.JRI.Rengine;
import org.testng.annotations.Test;

public class JRITest {

    public void scratchpad() {
        Job j = null;
        j.setJobName("a");
        Configuration conf = null;

    }

    @Test(enabled = false)
    public static void libpathTest() throws Exception {
        System.out.println(System.getProperty("java.library.path"));
    }

    @Test(enabled = false)
    public static void jriTest() throws Exception {
        String[] args = new String[]{"--vanilla"};

        Rengine engine = new Rengine(args, false, null);
        REXP r;

        r = engine.eval("library(ecor)");
        System.out.println(r.toString());

        r = engine.eval("stop('R error.')");

        r = engine.eval("T");
        System.out.println(r.getType() == REXP.XT_ARRAY_BOOL_INT);

        r = engine.eval("'abc'");
        System.out.println("" + r.getType() + (r.getType() == REXP.XT_STR));

        r = engine.eval("list(a=list(\"B\",\"D\", list(sublist=\"s\")))");
        System.out.println("" + r.getType() + (r.getType() == REXP.XT_VECTOR));

        RList rlist = r.asList();
        REXP axp = rlist.at("a");
        RList al = axp.asList();
        RVector av = axp.asVector();

        if (al != null)
            System.out.printf("%s\n",/*al.keys().length,*/al.at(0));
        if (av != null)
            System.out.printf("%d,%s\n", av.size(), av.at(0));


//        System.out.printf("%s,%s\n", r.toString(), r.);

        engine.end();

    }

}
