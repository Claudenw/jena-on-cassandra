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
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * 
 * Handles the Cassandra connection and table definitions.
 * 
 * Tables are identified by ID or name.  
 * 
 *
 */
public class CassandraConnection implements Closeable {

	/*
	 * List of tables named by key order
	 */
	private static final String SPOG = "SPOG";
	private static final String POGS = "POGS";
	private static final String OSGP = "OSGP";
	private static final String GSPO = "GSPO";
	
	/*
	 * List of column types/names
	 */
	/**
	 * The subject column name.
	 */
	public static final String COL_SUBJECT = "subject";
	/**
	 * The predicate column name.
	 */
	public static final String COL_PREDICATE = "predicate";
	/**
	 * The object column name.
	 */
	public static final String COL_OBJECT = "object";
	/**
	 * The graph column name.
	 */
	public static final String COL_GRAPH = "graph";

	private static final Log LOG = LogFactory.getLog(CassandraConnection.class);

	/*
	 * Map table IDs to names.
	 */
	private static final Map<String, String> TABLE_MAP = new HashMap<String, String>();
	/*
	 * List of IDs that need a filter to work correctly. 
	 */
	private static final Collection<String> NEEDS_FILTER = Arrays.asList(
			"_p_g", "__og");

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
		TABLE_MAP.put("_pog", POGS);
		TABLE_MAP.put("_po_", POGS);
		TABLE_MAP.put("_p_g", POGS); // + filter or GSPO + filter
		TABLE_MAP.put("_p__", POGS);
		TABLE_MAP.put("__og", OSGP); // + filter or GSPO + filter
		TABLE_MAP.put("__o_", OSGP);
		TABLE_MAP.put("___g", GSPO);
		TABLE_MAP.put("____", GSPO); // or any other
	}

	/* Cassandra Cluster. */
	private Cluster cluster;

	/* Cassandra Session. */
	private Session session;
	
	/**
	 * Build the table ID from the graph name and the triple pattern.
	 * 
	 * The graph ID string is "spog" with the letters replaced with underscores "_" if 
	 * the subject, predicate, object or graph is Node.ANY or null.
	 * Quad.isUnionGraph( graph ) is considered the same as grap = Node.ANY.
	 * @param graph The graph name.  
	 * @param triplePattern The triple to match.
	 * @return the graph ID string.
	 */
	public static String getId(Node graph, Triple triplePattern) {
		boolean s = triplePattern.getMatchSubject() != null;
		boolean p = triplePattern.getMatchPredicate() != null;
		boolean o = triplePattern.getMatchObject() != null;
		boolean g = graph != null && graph != Node.ANY && ! Quad.isUnionGraph(graph);
		return new StringBuilder().append(s ? 's' : '_').append(p ? 'p' : '_')
				.append(o ? 'o' : '_').append(g ? 'g' : '_').toString();
	}

	/**
	 * Constructor.
	 * @param contactPoint The contact point for the cassandra server.
	 * @param port the port for the cassandra server.
	 */
	public CassandraConnection(String contactPoint, int port) {
		this.cluster = Cluster.builder().addContactPoint(contactPoint)
				.withPort(port).build();
		this.session = cluster.connect();
	}

	/**
	 * Constructor. Uses port 9042
	 * @param contactPoint The contact point for the cassandra server.
	 */
	public CassandraConnection(String contactPoint) {
		this(contactPoint,9042);
	}

	@Override
	public void close() {
		cluster.close();
	}

	/**
	 * Get the list of tabales.
	 * @return The list of tables.
	 */
	public Collection<String> getTableList() {
		return Arrays.asList(GSPO, OSGP, POGS, SPOG);
	}

	/**
	 * Get the cassandra session.
	 * @return The cassandra session.
	 */
	public Session getSession() {
		return session;
	}

	/**
	 * Delete the tables we manage from the keyspace.
	 * @param keyspace The keyspace to delete from
	 */
	public void deleteTables(String keyspace) {
		for (String tbl : getTableList()) {
			String stmt = String.format("DROP TABLE IF EXISTS %s.%s",
					keyspace, tbl);
			LOG.debug(stmt);
			getSession().execute(stmt);
		}
	}

	/**
	 * Create the tables we manage in the keyspace.
	 * @param keyspace the keyspace to create the tables in.
	 */
	public void createTables(String keyspace) {
		for (String tbl : getTableList()) {
			String stmt = String
					.format("CREATE TABLE %s.%s (%s blob, %s blob, %s blob, %s blob, PRIMARY KEY %s)",
							keyspace, tbl, 
							COL_SUBJECT, COL_PREDICATE, COL_OBJECT, COL_GRAPH,
							primaryKey(tbl));
			LOG.debug(stmt);
			getSession().execute(stmt);
		}
	}

	/**
	 * Build the primary key for a table.
	 * @param tblName The table name
	 * @return the key definition for the table.
	 */
	private String primaryKey(String tblName) {
		StringBuilder sb = new StringBuilder("( ");
		for (int i = 0; i < tblName.length(); i++) {
			char c = tblName.charAt(i);
			switch (c) {
			case 'S':
				if (sb.length() > 2) {
					sb.append(", ");
				}
				sb.append("subject");
				break;
			case 'P':
				if (sb.length() > 2) {
					sb.append(", ");
				}
				sb.append("predicate");
				break;
			case 'O':
				if (sb.length() > 2) {
					sb.append(", ");
				}
				sb.append("object");
				break;
			case 'G':
				if (sb.length() > 2) {
					sb.append(", ");
				}
				sb.append("graph");
				break;
			default:
				throw new IllegalArgumentException(String.format(
						"Table name %s is invalid at character %s", tblName, c));
			}
		}
		return sb.append(" )").toString();
	}

	/**
	 * Get the table name for the ID.
	 * @param tableId the table id.
	 * @return the associated table name.
	 */
	public String getTable(String tableId) {
		String tblName = TABLE_MAP.get(tableId);
		if (tblName == null) {
			throw new IllegalStateException(String.format("No table for %s",
					tableId));
		}
		return tblName;
	}

	/**
	 * Return true if the specified table requries a filter.
	 * @param tableId The table Id.
	 * @return True if the table identified by the id needs a filter.
	 */
	public boolean needsFilter(String tableId) {
		return NEEDS_FILTER.contains(tableId);
	}
}
