package org.apache.jena.cassandra.graph;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.ConnectException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class CassandraJMXConnection implements AutoCloseable {
    private static final Log LOG = LogFactory.getLog(CassandraJMXConnection.class);
    private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://[%s]:%d/jmxrmi";
    private static final String ssObjName = "org.apache.cassandra.db:type=StorageService";
    private static final int defaultPort = 7199;

    final String host;
    final int port;
    private String username;
    private String password;

    private JMXConnector jmxc;
    private MBeanServerConnection mbeanServerConn;

    public interface MetricMBean
    {
        ObjectName objectName();
    }

    public interface JmxGaugeMBean extends MetricMBean
    {
        Object getValue();
    }


    /**
     * Creates a NodeProbe using the specified JMX host, port, username, and password.
     *
     * @param host hostname or IP address of the JMX agent
     * @param port TCP port of the remote JMX agent
     * @throws IOException on connection failures
     */
    private CassandraJMXConnection(RMIClientSocketFactory socketFactory, String host, int port, String username, String password) throws IOException
    {
        assert username != null && !username.isEmpty() && password != null && !password.isEmpty()
                : "neither username nor password can be blank";

        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        connect(socketFactory);
    }

    /**
     * Creates a NodeProbe using the specified JMX host and port.
     *
     * @param host hostname or IP address of the JMX agent
     * @param port TCP port of the remote JMX agent
     * @throws IOException on connection failures
     */
    private CassandraJMXConnection(RMIClientSocketFactory socketFactory, String host, int port) throws IOException
    {
        this.host = host;
        this.port = port;
        connect(socketFactory);
    }


    private void connect(RMIClientSocketFactory socketFactory) throws IOException
    {
        JMXServiceURL jmxUrl = new JMXServiceURL(String.format(fmtUrl, host, port));
        Map<String, Object> env = new HashMap<String, Object>();
        if (username != null)
        {
            String[] creds = { username, password };
            env.put(JMXConnector.CREDENTIALS, creds);
        }

        env.put("com.sun.jndi.rmi.factory.socket", socketFactory);

        jmxc = JMXConnectorFactory.connect(jmxUrl, env);
        mbeanServerConn = jmxc.getMBeanServerConnection();

        //        try
        //        {
        //            ObjectName name = new ObjectName(ssObjName);
        //            ssProxy = JMX.newMBeanProxy(mbeanServerConn, name, StorageServiceMBean.class);
        //            name = new ObjectName(MessagingService.MBEAN_NAME);
        //            msProxy = JMX.newMBeanProxy(mbeanServerConn, name, MessagingServiceMBean.class);
        //            name = new ObjectName(StreamManagerMBean.OBJECT_NAME);
        //            streamProxy = JMX.newMBeanProxy(mbeanServerConn, name, StreamManagerMBean.class);
        //            name = new ObjectName(CompactionManager.MBEAN_OBJECT_NAME);
        //            compactionProxy = JMX.newMBeanProxy(mbeanServerConn, name, CompactionManagerMBean.class);
        //            name = new ObjectName(FailureDetector.MBEAN_NAME);
        //            fdProxy = JMX.newMBeanProxy(mbeanServerConn, name, FailureDetectorMBean.class);
        //            name = new ObjectName(CacheService.MBEAN_NAME);
        //            cacheService = JMX.newMBeanProxy(mbeanServerConn, name, CacheServiceMBean.class);
        //            name = new ObjectName(StorageProxy.MBEAN_NAME);
        //            spProxy = JMX.newMBeanProxy(mbeanServerConn, name, StorageProxyMBean.class);
        //            name = new ObjectName(HintsService.MBEAN_NAME);
        //            hsProxy = JMX.newMBeanProxy(mbeanServerConn, name, HintsServiceMBean.class);
        //            name = new ObjectName(GCInspector.MBEAN_NAME);
        //            gcProxy = JMX.newMBeanProxy(mbeanServerConn, name, GCInspectorMXBean.class);
        //            name = new ObjectName(Gossiper.MBEAN_NAME);
        //            gossProxy = JMX.newMBeanProxy(mbeanServerConn, name, GossiperMBean.class);
        //            name = new ObjectName(BatchlogManager.MBEAN_NAME);
        //            bmProxy = JMX.newMBeanProxy(mbeanServerConn, name, BatchlogManagerMBean.class);
        //            name = new ObjectName(ActiveRepairServiceMBean.MBEAN_NAME);
        //            arsProxy = JMX.newMBeanProxy(mbeanServerConn, name, ActiveRepairServiceMBean.class);
        //        }
        //        catch (MalformedObjectNameException e)
        //        {
        //            throw new RuntimeException(
        //                    "Invalid ObjectName? Please report this as a bug.", e);
        //        }
        //
        //        memProxy = ManagementFactory.newPlatformMXBeanProxy(mbeanServerConn,
        //                ManagementFactory.MEMORY_MXBEAN_NAME, MemoryMXBean.class);
        //        runtimeProxy = ManagementFactory.newPlatformMXBeanProxy(
        //                mbeanServerConn, ManagementFactory.RUNTIME_MXBEAN_NAME, RuntimeMXBean.class);
    }


    @Override
    public void close() throws IOException
    {
        try
        {
            jmxc.close();
        }
        catch (ConnectException e)
        {
            // result of 'stopdaemon' command - i.e. if close() call fails, the daemon is shutdown
            LOG.error("Cassandra has shutdown?", e );
        }
    }

    public long getEstimatedSize(String keyspace, String tableName) {
        try {
            ObjectName oName = new ObjectName(String.format("org.apache.cassandra.metrics:type=%s,keyspace=%s,scope=%s,name=%s", "Table", keyspace, tableName, "EstimatedPartitionCount"));
            Object estimatedPartitionCount = JMX.newMBeanProxy(mbeanServerConn, oName, JmxGaugeMBean.class).getValue();
            // may return -1 for error
            return ((Long) estimatedPartitionCount).longValue();
        } catch (MalformedObjectNameException e) {
            LOG.error( "can not create ObjectName", e );
            return -1L;
        }
    }

    public static class Factory {
        public static final boolean SSH=true;
        public static final boolean NO_SSH=false;

        private static final Log LOG = LogFactory.getLog(Factory.class);
        public static int DEFAULT_PORT = 7199;
        private String username;
        private String password;
        private List<String> server = new ArrayList<String>();
        private int port = DEFAULT_PORT;
        private boolean useSSH = false;
        private Random random = new Random();

        public void withCredentials(String username, String password) {
            this.username = username;
            this.password = password;
        }

        public boolean hasCredentials() {
            return username != null && !username.isEmpty() && password != null && !password.isEmpty();
        }

        public void addContactPoint(String server) {
            this.server.add(server);
        }

        public boolean hasContactPoints() {
            return !this.server.isEmpty();
        }

        public void withSSH() {
            withSSH( true );
        }

        public void withSSH( boolean state ) {
            this.useSSH = state;
        }

        public void withPort(int port) {
            assert port > 0;
            this.port = port;
        }

        private String getServer() {
            if (server.isEmpty()) {
                throw new IllegalStateException( "No contact points provided");
            }
            return server.get(random.nextInt(server.size()));
        }

        private RMIClientSocketFactory getRMIClientSocketFactory()
        {
            return useSSH ?  new SslRMIClientSocketFactory() : RMISocketFactory.getDefaultSocketFactory();
        }


        public CassandraJMXConnection getConnection() throws IOException {
            return hasCredentials() ? new CassandraJMXConnection(getRMIClientSocketFactory(), getServer(), port, username, password)
                    : new CassandraJMXConnection(getRMIClientSocketFactory(), getServer(), port);
        }

        public boolean isValid( boolean sslFlag ) {
            Predicate<String> connectionFilter = new Predicate<String>() {

                private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://[%s]:%d/jmxrmi";

                private RMIClientSocketFactory getRMIClientSocketFactory()
                {
                    if (sslFlag)
                        return new SslRMIClientSocketFactory();
                    else
                        return RMISocketFactory.getDefaultSocketFactory();
                }

                @Override
                public boolean test(String host) {
                    try {
                        JMXServiceURL jmxUrl = new JMXServiceURL(String.format(fmtUrl, host, port));
                        Map<String, Object> env = new HashMap<String, Object>();
                        if (username != null)
                        {
                            String[] creds = { username, password };
                            env.put(JMXConnector.CREDENTIALS, creds);
                        }

                        env.put("com.sun.jndi.rmi.factory.socket", getRMIClientSocketFactory());

                        try( JMXConnector connection = JMXConnectorFactory.connect(jmxUrl, env) )
                        {
                            return true;
                        } catch (IOException e) {
                            return false;
                        }
                    } catch (MalformedURLException e) {
                        return false;
                    }
                }};

                server = server.stream().filter( connectionFilter ).collect( Collectors.toList());
                return !server.isEmpty();
        }
    }

}
