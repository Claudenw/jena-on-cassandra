package org.apache.jena.cassandra.graph;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.util.HashSet;
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
	private final int rcpPort;
	private EmbeddedCassandraService cassandra;
	//private final Thread thread;
	
	private final static String YAML_FMT = "%n%s: %s%s";
	
	public CassandraSetup() throws TTransportException, IOException {
		tempDir = Files.createTempDir();
		File storage = new File( tempDir, "storage");
		storage.mkdir();
		System.setProperty("cassandra.storagedir", tempDir.toString());

		File yaml = new File(tempDir, "cassandra.yaml");
		URL url = CassandraSetup.class.getClassLoader().getResource("cassandraTest.yaml");
		FileOutputStream yamlOut = new FileOutputStream(yaml);
		IOUtils.copy( url.openStream(), yamlOut );
		
		storagePort = findFreePort();
		yamlOut.write( String.format( YAML_FMT, "storage_port", storagePort).getBytes());

		sslStoragePort = findFreePort();
		yamlOut.write( String.format( YAML_FMT, "ssl_storage_port", sslStoragePort).getBytes());

		nativePort = findFreePort();
		yamlOut.write( String.format( YAML_FMT, "native_transport_port", sslStoragePort).getBytes());

		rcpPort = findFreePort();
		yamlOut.write( String.format( YAML_FMT, "rcp_port", sslStoragePort).getBytes());
		
		yamlOut.flush();
		yamlOut.close();
		System.setProperty("cassandra.config", yaml.toString());
		
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
    private static int findFreePort() {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            int port = socket.getLocalPort();
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore IOException on close()
            }
            return port;
        } catch (IOException e) {
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                }
            }
        }
        throw new IllegalStateException("Could not find a free TCP/IP port");
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

	public int getRcpPort() {
		return rcpPort;
	}
    
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
