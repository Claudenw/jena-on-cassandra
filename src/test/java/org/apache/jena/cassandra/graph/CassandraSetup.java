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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.commons.io.IOUtils;
import org.apache.jena.ext.com.google.common.io.Files;
import org.apache.thrift.transport.TTransportException;

public class CassandraSetup {
	private final File tempDir;
	private final int storagePort;
	private final int sslStoragePort;
	private final int nativePort;
	//private final int rcpPort;
	private EmbeddedCassandraService cassandra;
	//private final Thread thread;
	
	private final static String YAML_FMT = "%n%s: %s%n";
	
	public CassandraSetup() throws TTransportException, IOException {
		tempDir = Files.createTempDir();
		File storage = new File( tempDir, "storage");
		storage.mkdir();
		System.setProperty("cassandra.storagedir", tempDir.toString());

		File yaml = new File(tempDir, "cassandra.yaml");
		URL url = CassandraSetup.class.getClassLoader().getResource("cassandraTest.yaml");
		FileOutputStream yamlOut = new FileOutputStream(yaml);
		IOUtils.copy( url.openStream(), yamlOut );
		
		int[] ports = findFreePorts(3);
		storagePort = ports[0];
		yamlOut.write( String.format( YAML_FMT, "storage_port", storagePort).getBytes());

		sslStoragePort = ports[1];
		yamlOut.write( String.format( YAML_FMT, "ssl_storage_port", sslStoragePort).getBytes());

		nativePort = ports[2];
		yamlOut.write( String.format( YAML_FMT, "native_transport_port", sslStoragePort).getBytes());

//		rcpPort = ports[3];
//		yamlOut.write( String.format( YAML_FMT, "rcp_port", sslStoragePort).getBytes());
		
		yamlOut.flush();
		yamlOut.close();
		System.setProperty("cassandra.config", yaml.toURI().toString());
		
		cassandra = new EmbeddedCassandraService();
		cassandra.start();
		
	}
	
	
	public void shutdown() {
		cassandra = null;
		FileUtils.deleteRecursive( tempDir );
	}
	
	/**
     * Collects all data dirs and returns a set of String paths on the file system.
     *
     * @return
     */
    private Set<String> getDataDirs() {
        Set<String> dirs = new HashSet<>();
        for (String s : DatabaseDescriptor.getAllDataFileLocations()) {
            dirs.add(s);
        }
        //dirs.add(DatabaseDescriptor.getLogFileLocation());
        return dirs;
    }
	
	/**
     * Creates the data diurectories, if they didn't exist.
     * @throws IOException if directories cannot be created (permissions etc).
     */
    public void makeDirsIfNotExist() throws IOException {
        for (String s: getDataDirs()) {
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
        	for (int i=0;i<num;i++)
        	{
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
            for (ServerSocket s : sockets)
            {
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

//	public int getRcpPort() {
//		return rcpPort;
//	}
    
//	/*
//	 * @author Ran Tavory (rantav@gmail.com)
//	 *
//	 */
//	private static class EmbeddedCassandraService implements Runnable
//	{
//	 
//	    CassandraDaemon cassandraDaemon;
//	 
//	    public void init() throws TTransportException, IOException
//	    {
//	        cassandraDaemon = new CassandraDaemon();
//	        cassandraDaemon.init(null);
//	    }
//	 
//	    public void run()
//	    {
//	        cassandraDaemon.start();
//	    }
//	}


}
