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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.thrift.ThriftConvert;
import org.apache.jena.riot.thrift.wire.RDF_Term;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.transport.TTransportException;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.utils.Bytes;


/**
 *
 * Handles the Cassandra cluster connection and table definitions.
 *
 * This class also manages the sessions for each keyspace.
 *
 * Tables are identified by ID or name.
 *
 *
 */
public class CassandraConnection implements Closeable {

    private static final Log LOG = LogFactory.getLog(CassandraConnection.class);

    /*
     * Map table IDs to names.
     */
    private static final Map<String, TableName> TABLE_MAP = new HashMap<String, TableName>();

    /*
     * List of tables named by key order
     */
    public static final TableName SPOG = new TableName("SPOG");
    public static final TableName PGOS = new TableName("PGOS");
    public static final TableName OSGP = new TableName("OSGP");
    public static final TableName GSPO = new TableName("GSPO");
    public static final TableName[] TABLES = { SPOG, PGOS, OSGP, GSPO };
    public static final TableName COUNT_TABLE = OSGP;

    /*
     * The mapping of table ID to name.
     */
    static {
        TABLE_MAP.put("spog", SPOG);
        TABLE_MAP.put("spo_", SPOG);
        TABLE_MAP.put("sp_g", GSPO);
        TABLE_MAP.put("sp__", SPOG);
        TABLE_MAP.put("s_og", OSGP);
        TABLE_MAP.put("s_o_", OSGP);
        TABLE_MAP.put("s__g", GSPO);
        TABLE_MAP.put("s___", SPOG);
        TABLE_MAP.put("_pog", PGOS);
        TABLE_MAP.put("_po_", PGOS);
        TABLE_MAP.put("_p_g", PGOS);
        TABLE_MAP.put("_p__", PGOS);
        TABLE_MAP.put("__og", OSGP); // + filter
        TABLE_MAP.put("__o_", OSGP);
        TABLE_MAP.put("___g", GSPO);
        TABLE_MAP.put("____", GSPO); // or any other
    }

    // /* ALTERNATE CONFIGURATION
    // * Object must be on the end. */
    // private static final TableName SPGO = new TableName("SPGO");
    // private static final TableName PGSO = new TableName("PGSO");
    // private static final TableName GSPO = new TableName("GSPO");
    // private static final TableName[] TABLES = { SPGO, PGSO, GSPO };
    //
    // static {
    // TABLE_MAP.put("spog", SPGO);
    // TABLE_MAP.put("spo_", SPGO);
    // TABLE_MAP.put("sp_g", SPGO);
    // TABLE_MAP.put("sp__", SPGO);
    // TABLE_MAP.put("s_og", GSPO); // needs filter
    // TABLE_MAP.put("s_o_", SPGO); // needs filter
    // TABLE_MAP.put("s__g", GSPO);
    // TABLE_MAP.put("s___", SPGO);
    // TABLE_MAP.put("_pog", PGSO); // needs filter
    // TABLE_MAP.put("_po_", PGSO); // need filter
    // TABLE_MAP.put("_p_g", PGSO);
    // TABLE_MAP.put("_p__", PGSO);
    // TABLE_MAP.put("__og", GSPO); // needs filter
    // TABLE_MAP.put("__o_", PGSO); // needs filter
    // TABLE_MAP.put("___g", GSPO);
    // TABLE_MAP.put("____", GSPO); // or any other
    // }
    //
    // private static final Collection<String> NEEDS_FILTER =
    // Arrays.asList("s_og", "s_o_", "_pog",
    // "_po_","__og", "__o_");


    /*
     * A thrift serializer
     */
    private final TSerializer ser;

    /* Cassandra Cluster. */
    private final Cluster cluster;

    /* Cassandra Session. */
    private final Map<String, Session> sessions;

    private final CassandraJMXConnection.Factory jmxFactory;

    /**
     * Build the table ID from the graph name and the triple pattern.
     *
     * The graph ID string is "spog" with the letters replaced with underscores
     * "_" if the subject, predicate, object or graph is Node.ANY or null.
     * Quad.isUnionGraph( graph ) is considered the same as graph = Node.ANY.
     *
     * @param quad
     *            The quad to find the id for.
     * @return the graph ID string.
     */
    public static String getId(Quad quad) {
        return new StringBuilder().append(ColumnName.S.getId(quad)).append(ColumnName.P.getId(quad))
                .append(ColumnName.O.getId(quad)).append(ColumnName.G.getId(quad)).toString();

    }

    /**
     * Constructor
     * @param cluster the cassandra cluster to use.
     * @throws TTransportException
     */
    public CassandraConnection(Cluster cluster, CassandraJMXConnection.Factory jmxFactory) throws TTransportException {
        this.cluster = cluster;
        this.sessions = Collections.synchronizedMap(new HashMap<String, Session>());
        this.jmxFactory = jmxFactory;
        ser = new TSerializer();
    }

    @Override
    public void close() {
        synchronized (sessions) {
            for (Session session : sessions.values()) {
                session.close();
            }
            sessions.clear();
        }
    }

    /**
     * Create the keyspace in the cluster.
     * @param createStmt the statement to execute.
     */
    public void createKeyspace(String createStmt) {
        Session session = null;
        try{
            session = cluster.connect();
            session.execute( createStmt );
        } finally {
            session.close();
        }
    }

    /**
     * Get the list of tabales.
     *
     * @return The list of tables.
     */
    public static Collection<TableName> getTableList() {
        return Arrays.asList(TABLES);
    }

    /**
     * Get the cassandra session.
     *
     * @return The cassandra session.
     */
    public Session getSession(String keyspace) {
        Session retval = sessions.get(keyspace);
        if (retval == null) {
            retval = cluster.connect(keyspace);
            sessions.put(keyspace, retval);
        }
        return retval;
    }

    /**
     * Delete the tables we manage from the keyspace.
     *
     * @param keyspace
     *            The keyspace to delete from
     */
    public void deleteTables(String keyspace) {
        ExtendedIterator<String> iter = null;
        for (TableName tbl : getTableList()) {
            if (iter == null) {
                iter = WrappedIterator.create(tbl.getDeleteTableStatements());
            } else {
                iter = iter.andThen(tbl.getDeleteTableStatements());
            }
        }
        executeUpdateSet(keyspace, iter);
    }

    /**
     * Create the tables we manage in the keyspace.
     *
     * @param keyspace
     *            the keyspace to create the tables in.
     */
    public void createTables(String keyspace) {

        Session session = getSession(keyspace);
        for (TableName tbl : getTableList()) {
            for (String stmt : tbl.getCreateTableStatements()) {
                LOG.debug(stmt);
                session.execute(stmt);
            }
        }
    }

    /**
     * Truncate all the tables in the keyspace.
     *
     * @param keyspace
     *            The keyspace to delete from.
     */
    public void truncateTables(String keyspace) {

        Iterator<String> statements = CassandraConnection.getTableList().stream()
                .map(new Function<TableName, String>() {
                    @Override
                    public String apply(TableName t) {
                        return String.format("TRUNCATE %s ;", t.getName());
                    }
                }).iterator();
        executeUpdateSet(keyspace, statements);
    }

    /**
     * Perform update statements (no data retrieval) using async calls.
     *
     * Method returns when all
     *  the async statements have been executed.  There is no guarantee that the statements
     *  will be executed in any particular order.
     *
     * @param keyspace The keyspace to execute the commands in.
     * @param statements An iterator of queries to execute.
     */
    public void executeUpdateSet(String keyspace, Iterator<String> statements) {
        BulkExecutor bulkExecutor = new BulkExecutor(getSession(keyspace));
        bulkExecutor.execute(statements);
        bulkExecutor.awaitFinish();
    }

    /**
     * Get the table name for the ID.
     *
     * @param tableId
     *            the table id.
     * @return the associated table name.
     */
    public static TableName getTable(String tableId) {
        TableName tblName = TABLE_MAP.get(tableId);
        if (tblName == null) {
            throw new IllegalStateException(String.format("No table for %s", tableId));
        }
        return tblName;
    }

    /**
     * Execute the query and return the result set.  Log any errors.
     * @param keyspace The keyspace to execute the query in.
     * @param query The query to execute
     * @return The Cassandra ResultSet from the query.
     */
    public ResultSet executeQuery(String keyspace, String query) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("executing query: " + query);
        }
        try {
            return getSession(keyspace).execute(query);
        } catch (QueryValidationException e) {
            LOG.error(String.format("Query Execution issue (%s) while executing: (%s)", e.getMessage(), query), e);
            throw e;
        }
    }

    /**
     * Execute a single update statement (no data returned).  Logging is performed as appropriate.
     * @param keyspace The keyspace to execute in.
     * @param statement the statement to execute.
     * @return ResultSetFuture
     */
    public ResultSetFuture executeUpdate(String keyspace, String statement) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("executing query: " + statement);
        }
        try {
            return getSession(keyspace).executeAsync(statement);
        } catch (QueryValidationException e) {
            LOG.error(String.format("Query Execution issue (%s) while executing: (%s)", e.getMessage(), statement), e);
            throw e;
        }
    }

    /**
     * Return the serialized value of the node.
     *
     * @param node
     *            the node to serialize.
     * @return The serialized node in a string form for use in cassandra
     *         queries.
     * @throws TException
     *             on serialization error.
     */
    public String valueOf(Node node) throws TException {
        RDF_Term term = new RDF_Term();
        ThriftConvert.toThrift(node, null, term, false);
        byte[] bary = ser.serialize(term);
        return Bytes.toHexString(bary);
    }

    /**
     * Returns an estimated count or -1 if ther was an error
     * @param keyspace the keyspace to search.
     * @param graph the Graph to count,  Node.ANY for union grpah.
     * @return an estimated count or -1 if ther was an error.
     */
    public long estimateTableSize(String keyspace, Node graph) {
        // All tables have the same size. They are just different organizations of the
        // same data.
        long result = -1;
        if (jmxFactory != null && graph.equals( Node.ANY )) {

            try (CassandraJMXConnection jmxConnection = jmxFactory.getConnection()) {
                result = jmxConnection.getEstimatedSize(keyspace, COUNT_TABLE.getName());
            } catch (IOException e) {
                LOG.error( "Can not connect to JMX server", e );
            }
        }
        if (result == -1) {
            QueryPattern pattern = new QueryPattern(this, graph, Triple.ANY);
            try {
                result = pattern.getCount(keyspace);
            } catch (TException e) {
                LOG.error("Error executing count", e);
            }
        }
        return result;
    }

}
