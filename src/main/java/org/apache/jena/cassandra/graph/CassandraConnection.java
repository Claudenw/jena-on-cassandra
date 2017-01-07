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
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.thrift.ThriftConvert;
import org.apache.jena.riot.thrift.wire.RDF_Term;
import org.apache.jena.sparql.core.Quad;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
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
	private final Session session;

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
	 * Constructor.
	 * 
	 * @param contactPoint
	 *            The contact point for the cassandra server.
	 * @param port
	 *            the port for the cassandra server.
	 */
	public CassandraConnection(String contactPoint, int port) {
		this.cluster = Cluster.builder().addContactPoint(contactPoint).withPort(port).build();
		this.session = cluster.connect();
	}

	/**
	 * Constructor. Uses port 9042
	 * 
	 * @param contactPoint
	 *            The contact point for the cassandra server.
	 */
	public CassandraConnection(String contactPoint) {
		this(contactPoint, 9042);
	}

	public CassandraConnection(Cluster cluster) {
		this.cluster = cluster;
		this.session = cluster.newSession();
	}

	@Override
	public void close() {
		cluster.close();
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
	public Session getSession() {
		return session;
	}

	/**
	 * Delete the tables we manage from the keyspace.
	 * 
	 * @param keyspace
	 *            The keyspace to delete from
	 */
	public void deleteTables(String keyspace) {
		for (TableName tbl : getTableList()) {
			String stmt = String.format("DROP TABLE IF EXISTS %s.%s", keyspace, tbl);
			LOG.debug(stmt);
			getSession().execute(stmt);
		}
	}

	/**
	 * Create the tables we manage in the keyspace.
	 * 
	 * @param keyspace
	 *            the keyspace to create the tables in.
	 */
	public void createTables(String keyspace) {
		for (TableName tbl : getTableList()) {
			for (String stmt : tbl.getCreateTableStatements(keyspace)) {
				LOG.debug(stmt);
				getSession().execute(stmt);
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

		StringBuilder sb = new StringBuilder("BEGIN BATCH").append(System.lineSeparator());
		for (TableName tableName : CassandraConnection.getTableList()) {
			sb.append(String.format("TRUNCATE %s.%s ;%n", keyspace, tableName.getName()));
		}
		session.execute(sb.append("APPLY BATCH;").toString());
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

	public ResultSet executeQuery(String query) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("executing query: " + query);
		}
		try {
			return getSession().execute(query);
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
