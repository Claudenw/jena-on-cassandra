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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.thrift.ThriftConvert;
import org.apache.jena.riot.thrift.wire.RDF_Term;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.utils.Bytes;

/**
 * 
 * Handles the Cassandra connection and table definitions.
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
	private final TSerializer ser = new TSerializer();

	/* Cassandra Cluster. */
	private final Cluster cluster;

	/* Cassandra Session. */
	private final Map<String,Session>  sessions;

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

	public CassandraConnection(Cluster cluster) {
		this.cluster = cluster;		
		this.sessions = Collections.synchronizedMap(new HashMap<String,Session>());
	}

	@Override
	public void close() {
		synchronized (sessions)
		{
			for (Session session : sessions.values())
			{
				session.close();
			}
			sessions.clear();
		}
	}
	
	public void createKeyspace( String createStmt )
	{
		cluster.connect().execute(createStmt);
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
		if (retval == null)
		{
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
			if (iter == null)
			{
				iter = WrappedIterator.create( tbl.getDeleteTableStatements());					
			}
			else
			{
				iter = iter.andThen( tbl.getDeleteTableStatements() );
			}
		}
		executeUpdateSet( keyspace, iter );
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
				
		Iterator<String> queries = CassandraConnection.getTableList().stream().map( new Function<TableName,String>(){
			@Override
			public String apply(TableName t) {
				return String.format("TRUNCATE %s ;", t.getName());
			}}).iterator();
		executeUpdateSet( keyspace, queries );
	}
	
	public void executeUpdateSet( String keyspace, Iterator<String> queries )
	{
		Session session = getSession(keyspace);
		ConcurrentHashMap<Runnable, ResultSetFuture> map = new ConcurrentHashMap<>();
		ExecutorService executor = Executors.newSingleThreadExecutor();
		//ForkJoinPool executor = new ForkJoinPool(1);
		try {
			while (queries.hasNext()) {
				String query = queries.next();

					Runnable runner = new Runnable() {
						@Override
						public void run() {
							LOG.debug( "finished executing query: "+query);
							map.remove(this);
						}
					};
					if (LOG.isDebugEnabled()) {
						LOG.debug("executing query: " + query);
					}
					ResultSetFuture rsf = session.executeAsync(query);
					map.put(runner, rsf);
					rsf.addListener(runner, executor);			
			}
			while (!map.isEmpty()) {
				Thread.yield();
			}
		} finally {
			executor.shutdown();
		}

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

	public ResultSetFuture executeUpdate(String keyspace, String query) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("executing query: " + query);
		}
		try {
			return getSession(keyspace).executeAsync(query);
		} catch (QueryValidationException e) {
			LOG.error(String.format("Query Execution issue (%s) while executing: (%s)", e.getMessage(), query), e);
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

}
