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
	private static final TableName SPOG = new TableName("SPOG");
	private static final TableName POGS = new TableName("POGS");
	private static final TableName OSGP = new TableName("OSGP");
	private static final TableName GSPO = new TableName("GSPO");

	private static final Log LOG = LogFactory.getLog(CassandraConnection.class);

	/*
	 * Map table IDs to names.
	 */
	private static final Map<String, TableName> TABLE_MAP = new HashMap<String, TableName>();
	/*
	 * List of IDs that need a filter to work correctly.
	 */
	private static final Collection<String> NEEDS_FILTER = Arrays.asList("_p_g", "__og");

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
	 * The graph ID string is "spog" with the letters replaced with underscores
	 * "_" if the subject, predicate, object or graph is Node.ANY or null.
	 * Quad.isUnionGraph( graph ) is considered the same as grap = Node.ANY.
	 * 
	 * @param graph
	 *            The graph name.
	 * @param triplePattern
	 *            The triple to match.
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
		return Arrays.asList(GSPO, OSGP, POGS, SPOG);
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
			String stmt = String.format("CREATE TABLE %s.%s (%s blob, %s blob, %s blob, %s blob, PRIMARY KEY %s)",
					keyspace, tbl, ColumnName.S, ColumnName.P, ColumnName.O, ColumnName.G, tbl.getPrimaryKey());
			LOG.debug(stmt);
			getSession().execute(stmt);
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

	/**
	 * Return true if the specified table requries a filter.
	 * 
	 * @param tableId
	 *            The table Id.
	 * @return True if the table identified by the id needs a filter.
	 */
	public static boolean needsFilter(String tableId) {
		return NEEDS_FILTER.contains(tableId);
	}

//	public static TableName findTableWithColumns(int pos, List<ColumnName> cols) {
//		int[] ratings = new int[4];
//		List<TableName> tables = new ArrayList<TableName>(Arrays.asList(SPOG, POGS, OSGP, GSPO));
//		StringBuilder sb = new StringBuilder().append( cols.get(pos).getChar());
//		for (int i = 0; i < pos; i++) {
//			ColumnName col = cols.get(i);
//			sb.append(col.getChar());
//			for (int j = 0; j < 4; j++) {
//				if (tables.get(j).getName().substring(0, pos - 1).indexOf(col.getChar()) > -1) {
//					ratings[j]++;
//				}
//				if (tables.get(j).getName().charAt(pos) == cols.get(pos).getChar()) {
//					ratings[j]++;
//				}
//			}
//		}
//		for (int i = pos; i < cols.size(); i++) {
//			for (int j = 0; j < 4; j++) {
//				if (tables.get(j).getName().charAt(i) == cols.get(pos).getChar()) {
//					ratings[j]++;
//				}
//				if (tables.get(j).getName().charAt(pos) == cols.get(pos).getChar()) {
//					ratings[j]++;
//				}
//			}
//		}
//
//		String colStr = sb.toString();
//		for (int j = 0; j < 4; j++) {
//			if (colStr.indexOf(tables.get(j).getName().charAt(0)) == -1) {
//				ratings[j] = -1;
//			}
//		}
//
//		int max = 0;
//		for (int i = 1; i < 4; i++) {
//			if (ratings[max] < ratings[i]) {
//				max = i;
//			}
//		}
//		return tables.get(max);
//	}
}
