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

package org.apache.jena.cassandra;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.cassandra.assembler.CassandraClusterAssembler;
import org.apache.jena.cassandra.graph.CassandraJMXConnection;
import com.datastax.driver.core.Cluster;
import org.xenei.docker.utils.Compose;

/**
 * A class to properly setup the testing Cassandra instance.
 *
 * This is a cassandra instance that runs in a temp directory and is destroyed
 * at the end of testing.
 *
 */
public class CassandraSetup implements AutoCloseable {
    private File tempDir;
    private int storagePort;
    private int sslStoragePort;
    private int nativePort;
    private Compose compose;
    private static final Log LOG = LogFactory.getLog(CassandraSetup.class);
    private final static String YAML_FMT = "%n%s: %s%n";
    private Cluster cluster;
    private CassandraJMXConnection.Factory jmxFactory;

    /**
     * Constructor.
     *
     * @throws IOException
     *             on IO error
     * @throws InterruptedException
     *             if initialization is interrupted.
     * @throws TimeoutException
     */
    public CassandraSetup() throws IOException, InterruptedException, TimeoutException {
        Compose.start();
        cluster = CassandraClusterAssembler.getCluster("testing", "localhost", 9042);
        jmxFactory = new CassandraJMXConnection.Factory();
        jmxFactory.addContactPoint("localhost");
        if (!jmxFactory.isValid(CassandraJMXConnection.Factory.NO_SSH)) {
            jmxFactory = null;
        }
    }


    @Override
    public void close() throws IOException, InterruptedException {
        cluster.close();
        Compose.stop();
    }

    /**
     * Get the cluster for this setup.
     *
     * @return the cluster.
     */
    public Cluster getCluster() {
        return cluster;
    }

    public CassandraJMXConnection.Factory getJMXFactory() throws IOException {
        return jmxFactory;
    }

    /**
     * Clean up after the testing setup.
     */
    public void shutdown() {
        try {
            close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Returns a free port number on localhost.
     *
     * Heavily inspired from org.eclipse.jdt.launching.SocketUtil (to avoid a
     * dependency to JDT just because of this). Slightly improved with close()
     * missing in JDT. And throws exception instead of returning -1.
     *
     * @return a free port number on localhost
     * @throws IllegalStateException
     *             if unable to find a free port
     */
    private int[] findFreePorts(int num) {
        int[] retval = new int[num];
        List<ServerSocket> sockets = new ArrayList<ServerSocket>();
        ServerSocket socket = null;
        try {
            for (int i = 0; i < num; i++) {
                socket = new ServerSocket(0);
                socket.setReuseAddress(true);
                retval[i] = socket.getLocalPort();
                sockets.add(socket);
                socket = null;
            }
            return retval;
        } catch (IOException e) {
        } finally {

            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                }
            }
            for (ServerSocket s : sockets) {
                try {
                    s.close();
                } catch (IOException e) {
                }
            }
        }
        throw new IllegalStateException(String.format("Could not find %s free TCP/IP port(s)", num));
    }

    /**
     * get the temp directory used by this setup. This is where all the data for
     * the instance is stored.
     *
     * @return The temp directory.
     */
    public File getTempDir() {
        return tempDir;
    }

    /**
     * get the port that cassandra is using for the storage engine.
     *
     * @return the port for the storage engine.
     */
    public int getStoragePort() {
        return storagePort;
    }

    /**
     * get the port that cassandra is using for the ssl storage engine.
     *
     * @return the port for the ssl storage engine.
     */
    public int getSslStoragePort() {
        return sslStoragePort;
    }

    /**
     * get the port that cassandra is using for the native interface.
     *
     * @return the port for the native interface.
     */
    public int getNativePort() {
        return nativePort;
    }

}
