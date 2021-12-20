/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jena.cassandra.graph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.cassandra.assembler.CassandraClusterAssembler;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

/**
 * Class to perform bulk operations on the Cassandra database.
 *
 */
public class BulkExecutor {

    /*
     * the Cassandra session to use.
     */
    // private Session session;

    /*
     * this is a map of all ResultSetFutures and the Runnables that are registered
     * as their listener. when the future completes the runnable removes the future
     * from the map.
     */
    private final ConcurrentHashMap<Runnable, ResultSetFuture> map;

    /*
     * The service that executes the runnables.
     */
    private final ExecutorService executor;

    /**
     * Constructor.
     *
     * @param session
     *            The Cassandra session to use.
     */
    public BulkExecutor(int threadCount) {
        this.executor = Executors.newFixedThreadPool(threadCount);
        this.map = new ConcurrentHashMap<>();
    }

    /**
     * Execute a number of statements.
     *
     * All output from the statements are discarded.
     *
     * Statement order is not guaranteed.
     *
     * May be called multiple times. May not be called after awaitFinish().
     *
     * @see awaitFinish
     *
     * @param statements
     *            An iterator of statements to execute.
     */
    public ExecList execute(Log log, Session session, Iterator<String> statements) {
        List<ResultSetFuture> lst = new ArrayList<ResultSetFuture>();
        while (statements.hasNext()) {
            String statement = statements.next();

            lst.add(session.executeAsync(statement));
        }
        return new ExecList(lst);
    }

    public Future<?> execute( Runnable r ) {
        return executor.submit(r);
    }

    public void shutdown() {
        executor.shutdown();
    }

    public class ExecList implements Future<List<ResultSetFuture>> {
        private List<ResultSetFuture> lst;

        ExecList(List<ResultSetFuture> lst) {
            this.lst = lst;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean flg = false;
            for (ResultSetFuture rsf : lst) {
                if (!rsf.isDone()) {
                    rsf.cancel(mayInterruptIfRunning);
                    flg = true;
                }
            }
            return flg;
        }

        @Override
        public List<ResultSetFuture> get() throws InterruptedException, ExecutionException {
            return lst;
        }

        @Override
        public List<ResultSetFuture> get(long arg0, TimeUnit arg1)
                throws InterruptedException, ExecutionException, TimeoutException {
            return lst;
        }

        @Override
        public boolean isCancelled() {
            for (ResultSetFuture rsf : lst) {
                if (!rsf.isCancelled()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean isDone() {
            for (ResultSetFuture rsf : lst) {
                if (!rsf.isDone()) {
                    return false;
                }
            }
            return true;
        }

        public void awaitFinish() {
            class X implements Runnable {

                public int remaining=lst.size();

                @Override
                public void run() {
                    remaining--;
                    if (remaining <= 0) {
                        synchronized(this) {
                            this.notify();
                        }
                    }
                }
            };

            X counter = new X();
            lst.forEach( f -> f.addListener( counter , executor));
            try {
                synchronized( counter ) {
                    counter.wait(1000);
                }
            } catch (InterruptedException e) {
                LogFactory.getLog( this.getClass() ).warn( e );
            }
        }

        public void executeAfter(Runnable r) {

            awaitFinish();

            r.run();
        }
    }
}
