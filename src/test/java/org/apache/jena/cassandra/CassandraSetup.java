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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.cassandra.assembler.CassandraClusterAssembler;
import org.apache.jena.ext.com.google.common.io.Files;
import org.apache.thrift.transport.TTransportException;

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

	public CassandraSetup() throws IOException, InterruptedException {
		setupCassandraDaemon();
		cluster = CassandraClusterAssembler.getCluster("testing", "localhost", sslStoragePort );
	}
	
	public Cluster getCluster() {
		return cluster;
	}
	
	private void setupCassandraDaemon() throws IOException, InterruptedException {
		if (cassandraDaemon != null)
		{
			String portStr[] = System.getProperty("CassandraSetup_Ports").split(",");
			storagePort = Integer.valueOf( portStr[0]);
			sslStoragePort = Integer.valueOf( portStr[1]);
			nativePort = Integer.valueOf( portStr[2]);	
			tempDir = new File( System.getProperty("CassandraSetup_Dir") );
			LOG.info( String.format("Cassandra dir: %s storage:%s ssl:%s native:%s", tempDir, storagePort, sslStoragePort, nativePort));
			LOG.info( "Cassandra already running.");
		}
		else {

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
		yamlOut.write(String.format(YAML_FMT, "native_transport_port", sslStoragePort).getBytes());

		yamlOut.flush();
		yamlOut.close();
		System.setProperty("cassandra.config", yaml.toURI().toString());
		System.setProperty("CassandraSetup_Ports", String.format( "%s,%s,%s", storagePort,sslStoragePort,nativePort));
		System.setProperty("CassandraSetup_Dir", tempDir.toString());

		LOG.info( String.format("Cassandra dir: %s storage:%s ssl:%s native:%s", tempDir, storagePort, sslStoragePort, nativePort));
		
		CountDownLatch latch = new CountDownLatch(1);
		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.execute( new Runnable() {

            @Override
            public void run() {
                cassandraDaemon = new CassandraDaemon(true);
                makeDirsIfNotExist();
                cassandraDaemon.activate();
                latch.countDown();
                Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        cassandraDaemon.deactivate();
                        if (tempDir != null)
						{
							FileUtils.deleteRecursive(tempDir);
						}
                        LOG.warn( "Cassandra stopped");
                }}));
            }
        });
		
		LOG.info( "Waiting for Cassandra to start");
		latch.await();
		LOG.info( "Cassandra started");
		}

	}

	public void shutdown() {

	}

	/**
	 * Collects all data dirs and returns a set of String paths on the file
	 * system.
	 *
	 * @return
	 */
	private Set<String> getDataDirs() {
		Set<String> dirs = new HashSet<>();
		for (String s : DatabaseDescriptor.getAllDataFileLocations()) {
			dirs.add(s);
		}
		// dirs.add(DatabaseDescriptor.getLogFileLocation());
		return dirs;
	}

	/**
	 * Creates the data diurectories, if they didn't exist.
	 * 
	 * @throws IOException
	 *             if directories cannot be created (permissions etc).
	 */
	public void makeDirsIfNotExist()  {
		for (String s : getDataDirs()) {
			FileUtils.createDirectory(s);
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

	public File getTempDir() {
		return tempDir;
	}

	public int getStoragePort() {
		return storagePort;
	}

	public int getSslStoragePort() {
		return sslStoragePort;
	}

	public int getNativePort() {
		return nativePort;
	}

}
