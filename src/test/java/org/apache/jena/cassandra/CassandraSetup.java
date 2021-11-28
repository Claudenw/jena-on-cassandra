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
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.cassandra.assembler.CassandraClusterAssembler;
import org.apache.jena.cassandra.assembler.CassandraNodeProbeAssembler;
import org.apache.jena.cassandra.graph.CassandraConnection.NodeProbeConfig;
import org.apache.jena.ext.com.google.common.io.Files;
import com.datastax.driver.core.Cluster;

/**
 * A class to properly setup the testing Cassandra instance.
 *
 * This is a cassandra instance that runs in a temp directory and is destroyed
 * at the end of testing.
 *
 */
public class CassandraSetup {
	private File tempDir;
	private int storagePort;
	private int sslStoragePort;
	private int nativePort;
	private static CassandraDaemon cassandraDaemon;
	private static final Log LOG = LogFactory.getLog(CassandraSetup.class);
	private final static String YAML_FMT = "%n%s: %s%n";
	private Cluster cluster;
	private NodeProbeConfig nodeProbeConfig;

	/**
	 * Constructor.
	 *
	 * @throws IOException
	 *             on IO error
	 * @throws InterruptedException
	 *             if initialization is interrupted.
	 */
	public CassandraSetup() throws IOException, InterruptedException {
		setupCassandraDaemon();
		cluster = CassandraClusterAssembler.getCluster("testing", "localhost", sslStoragePort);
		nodeProbeConfig = CassandraNodeProbeAssembler.getNodeProbeConfig("testing", "localhost", NodeProbeConfig.DEFAULT_PORT, false);
	}

	/**
	 * Get the cluster for this setup.
	 *
	 * @return the cluster.
	 */
	public Cluster getCluster() {
		return cluster;
	}

	public NodeProbeConfig getNodeProbeConfig() {
	    return nodeProbeConfig;
	}

	/**
	 * Set up the daemon or detect that it is already running.
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void setupCassandraDaemon() throws IOException, InterruptedException {
		if (cassandraDaemon != null) {
			String portStr[] = System.getProperty("CassandraSetup_Ports").split(",");
			storagePort = Integer.valueOf(portStr[0]);
			sslStoragePort = Integer.valueOf(portStr[1]);
			nativePort = Integer.valueOf(portStr[2]);
			tempDir = new File(System.getProperty("CassandraSetup_Dir"));
			LOG.info(String.format("Cassandra dir: %s storage:%s ssl:%s native:%s", tempDir, storagePort,
					sslStoragePort, nativePort));
			LOG.info("Cassandra already running.");
		} else {

			tempDir = Files.createTempDir();
			File storage = new File(tempDir, "storage");
			storage.mkdir();
			System.setProperty("cassandra.storagedir", tempDir.toString());

			File yaml = new File(tempDir, "cassandra.yaml");
			URL url = CassandraSetup.class.getClassLoader().getResource("cassandraTest.yaml");
			FileOutputStream yamlOut = new FileOutputStream(yaml);
			IOUtils.copy(url.openStream(), yamlOut);

			int[] ports = findFreePorts(3);
			storagePort = ports[0];
			yamlOut.write(String.format(YAML_FMT, "storage_port", storagePort).getBytes());

			sslStoragePort = ports[1];
			yamlOut.write(String.format(YAML_FMT, "ssl_storage_port", sslStoragePort).getBytes());

			nativePort = ports[2];
			yamlOut.write(String.format(YAML_FMT, "native_transport_port", nativePort).getBytes());

			yamlOut.flush();
			yamlOut.close();
			System.setProperty("cassandra.config", yaml.toURI().toString());
			System.setProperty("CassandraSetup_Ports",
					String.format("%s,%s,%s", storagePort, sslStoragePort, nativePort));
			System.setProperty("CassandraSetup_Dir", tempDir.toString());
			System.setProperty("cassandra-foreground", "true");
			LOG.info(String.format("Cassandra dir: %s storage:%s ssl:%s native:%s", tempDir, storagePort,
					sslStoragePort, nativePort));

			CountDownLatch latch = new CountDownLatch(1);
			ExecutorService executor = Executors.newSingleThreadExecutor();
			executor.execute(new Runnable() {

				@Override
				public void run() {
					cassandraDaemon = new CassandraDaemon(true);
					cassandraDaemon.activate();
					latch.countDown();
					Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
						@Override
						public void run() {
							cassandraDaemon.deactivate();
							if (tempDir != null) {
								FileUtils.deleteRecursive(tempDir);
							}
							LOG.warn("Cassandra stopped");
						}
					}));
				}
			});

			LOG.info("Waiting for Cassandra to start");
			latch.await();
			LOG.info("Cassandra started");
		}

	}

	/**
	 * Clean up after the testing setup.
	 */
	public void shutdown() {

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
