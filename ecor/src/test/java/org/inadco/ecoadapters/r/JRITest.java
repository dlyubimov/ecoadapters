package org.inadco.ecoadapters.r;

import java.awt.FileDialog;
import java.awt.Frame;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RMainLoopCallbacks;
import org.rosuda.JRI.Rengine;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class JRITest {

    private Rengine re;
    

    @BeforeClass
    public void setup() throws Exception {

        System.out.println("java library path:"+System.getProperty("java.library.path"));
        
        re = new Rengine(new String[] { "--vanilla" }, false, null /*new TextConsole()*/);
        if (!re.waitForR())
            throw new IOException("Unable to access R engine.");

    }

    public void close() throws Exception {
    }

    @Test
    public void testJRIEngine() throws Exception {

        Map<String, Object> rlist = new HashMap<String, Object>(11);
        rlist.put("abc", "erwer");

        re.eval("library(rJava)");
        re.eval(".jinit()");

        REXP jobjRef = re.createRJavaRef(rlist);

        re.assign("rlist", jobjRef);
        REXP res = re.eval("rlist$get('abc')");

        System.out.printf("%s\n", res);

        Path[] paths = new Path[] { new Path ("/tmp/cache/abc.dat") };
        jobjRef = re.createRJavaRef(paths);
        
        re.assign("pathList",jobjRef);
        res = re.eval(".jevalArray(pathList)");
        
        System.out.printf("%s\n", res);

    }

    static class TextConsole implements RMainLoopCallbacks {
        public void rWriteConsole(Rengine re, String text, int oType) {
            System.out.print(text);
        }

        public void rBusy(Rengine re, int which) {
            System.out.println("rBusy(" + which + ")");
        }

        public String rReadConsole(Rengine re, String prompt, int addToHistory) {
            System.out.print(prompt);
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                String s = br.readLine();
                return (s == null || s.length() == 0) ? s : s + "\n";
            } catch (Exception e) {
                System.out.println("jriReadConsole exception: " + e.getMessage());
            }
            return null;
        }

        public void rShowMessage(Rengine re, String message) {
            System.out.println("rShowMessage \"" + message + "\"");
        }

        public String rChooseFile(Rengine re, int newFile) {
            FileDialog fd =
                new FileDialog(
                    new Frame(),
                    (newFile == 0) ? "Select a file" : "Select a new file",
                    (newFile == 0) ? FileDialog.LOAD : FileDialog.SAVE);
            fd.setVisible(true);
            String res = null;
            if (fd.getDirectory() != null)
                res = fd.getDirectory();
            if (fd.getFile() != null)
                res = (res == null) ? fd.getFile() : (res + fd.getFile());
            return res;
        }

        public void rFlushConsole(Rengine re) {
        }

        public void rLoadHistory(Rengine re, String filename) {
        }

        public void rSaveHistory(Rengine re, String filename) {
        }
    }

}
