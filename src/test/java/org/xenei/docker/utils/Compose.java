package org.xenei.docker.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.time.StopWatch;

public class Compose {

    private String fileName;

    private static class copyIO implements Runnable {
        private final InputStream in;
        private final OutputStream out;

        public copyIO(InputStream in, OutputStream out) {
            this.in  =in;
            this.out = out;
        }
        @Override
        public void run() {
            try {
                in.transferTo(out);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    private static void doIt( Process p ) throws InterruptedException {
        Thread out = new Thread( new copyIO( p.getInputStream(), System.out ));
        Thread err = new Thread( new copyIO( p.getErrorStream(), System.err ));
        out.start();
        err.start();
        p.waitFor();
    }

    public static void start() throws IOException, InterruptedException {
        if (System.getenv( "CassandraRunning") == null) {
            doIt( Runtime.getRuntime().exec( "./src/test/cassandra/start.sh") );
        }

    }


    public static void stop() throws IOException, InterruptedException {
        if (System.getenv( "CassandraRunning") == null) {
            doIt(Runtime.getRuntime().exec( "./src/test/cassandra/stop.sh"));
        }
    }


}
