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

import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.graph.Capabilities;
import org.apache.jena.graph.GraphEvents;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.AllCapabilities;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.sparql.core.Quad;
import org.apache.thrift.TException;

import com.datastax.driver.core.ResultSet;

import org.apache.jena.cassandra.graph.QueryPattern.RowToQuad;
import org.apache.jena.cassandra.graph.QueryPattern.FindNull;

/**
 * The Cassandra graph implementation.
 *
 */
public class GraphCassandra extends GraphBase {

	/*
	 * The name of the graph, or Node.ANY for union graph.
	 */
	private Node graph;
	/*
	 * The Cassandra connection
	 */
	private CassandraConnection connection;
	/*
	 * The graph capabilities.
	 */
	private Capabilities capabilities;
	/*
	 * The Cassandra keyspace that the tables are in.
	 */
	private final String keyspace;

	private static final Log LOG = LogFactory.getLog(GraphCassandra.class);

	/**
	 * Constructor.
	 * A null or Node.ANY for the graph name results in the UnionGraph being used.
	 * @param graph The name of the graph.
	 * @param keyspace The keyspace to used.
	 * @param connection The cassandra connection.
	 */
	public GraphCassandra(Node graph, String keyspace, CassandraConnection connection) {
		if (connection == null)
		{
			throw new IllegalArgumentException( "Connection may not be null");
		}
		if (StringUtils.isBlank(keyspace))
		{
			throw new IllegalArgumentException( "Keyspace may not be null");
		}
		this.graph = graph == null ? Node.ANY
				: (Quad.isUnionGraph(graph) ? Node.ANY : graph);
		this.connection = connection;
		this.keyspace = keyspace;
	}

	/**
	 * Constructor. Creates Union graph.
	 * @param keyspace The keyspace to used.
	 * @param connection The cassandra connection.
	 */
	public GraphCassandra(String keyspace, CassandraConnection connection) {
		this(null, keyspace, connection);
	}

	/**
	 * Get the graph name.
	 * May be Node.ANY
	 * @return The graph name.
	 */
	public Node getGraphName() {
		return graph;
	}

	@Override
	public void performAdd(Triple t) {
		if (Quad.isDefaultGraph(graph)) {
			throw new AddDeniedException("Can not add to default graph named "
					+ graph);
		}
		QueryPattern pattern = new QueryPattern(connection, graph, t);
		try {
			String values = pattern.getValues();
			for (String tbl : connection.getTableList()) {
				String stmt = String
						.format("INSERT INTO %s.%s (subject, predicate, object, graph) VALUES %s",
								keyspace, tbl, values);
				LOG.debug(stmt);
				connection.getSession().execute(stmt);
			}
		} catch (TException e) {
			LOG.error("bad values", e);
		}
	}

	@Override
	public void performDelete(Triple t) {
		if (Quad.isDefaultGraph(graph)) {
			throw new AddDeniedException(
					"Can not delete from default graph named " + graph);
		}
		QueryPattern pattern = new QueryPattern(connection, graph, t);
		try {

			for (String tbl : connection.getTableList()) {

				if (pattern.needsFilter(tbl)) {
					LOG.debug("Delete requires filtering");
					ExtendedIterator<Triple> iter = graphBaseFind(t);
					try {
						while (iter.hasNext()) {
							performDelete(iter.next());
						}
					} finally {
						iter.close();
					}
				} else {
					String stmt = String
							.format("DELETE subject, predicate, object, graph FROM %s.%s %s",
									keyspace, tbl,
									pattern.getWhereClause(tbl));
					LOG.debug(stmt);
					connection.getSession().execute(stmt);
				}

			}
		} catch (TException e) {
			LOG.error("bad values", e);
		}
	}

	@Override
	public void clear() {
		performDelete(Triple.ANY);
		getEventManager().notifyEvent(this, GraphEvents.removeAll);
	}

	@Override
	public boolean isClosed() {
		return connection.getSession().isClosed();
	}

	@Override
	public Capabilities getCapabilities() {
		if (capabilities == null)
			capabilities = new AllCapabilities() {
				@Override
				public boolean addAllowed(boolean every) {
					return !graph.equals(Node.ANY);
				}

				@Override
				public boolean deleteAllowed(boolean every) {
					return !graph.equals(Node.ANY);
				}

				@Override
				public boolean iteratorRemoveAllowed() {
					return false;
				}
			};
		return capabilities;
	}

	@Override
	public boolean isEmpty() {
		QueryPattern pattern = new QueryPattern(connection, graph, Triple.ANY);
		String tableName = pattern.getTableName();
		String columnName = pattern.getPartitionKey();
		StringBuilder whereClause = null;
		try {
			whereClause = pattern.getWhereClause();
		} catch (TException e) {
			LOG.error("Error building where clause", e);
			whereClause = new StringBuilder();
		}
		whereClause.append(whereClause.length() == 0 ? " WHERE " : " AND ")
				.append(String.format("token(%s)>%s", columnName,
						Long.MIN_VALUE));
		String query = String.format("SELECT token(%s) FROM %s.%s %s LIMIT 1",
				columnName, keyspace, tableName, whereClause);
		LOG.debug(query);
		ResultSet rs = connection.getSession().execute(query);
		return rs.one() == null;
	}

	@Override
	protected int graphBaseSize() {
		QueryPattern pattern = new QueryPattern(connection, graph, Triple.ANY);
		String tableName = pattern.getTableName();
		String columnName = pattern.getPartitionKey();
		StringBuilder whereClause = null;
		try {
			whereClause = pattern.getWhereClause(tableName);
		} catch (TException e) {
			LOG.error("Error building where clause", e);
			whereClause = new StringBuilder();
		}
		whereClause.append(whereClause.length() == 0 ? " WHERE " : " AND ")
				.append(String.format("token(%s)>%s", columnName,
						Long.MIN_VALUE));
		String query = String.format("SELECT count(*) FROM %s.%s %s",
				columnName, keyspace, tableName, whereClause);
		LOG.debug(query);
		ResultSet rs = connection.getSession().execute(query);
		return rs.one().getInt(0);
	}

	@Override
	protected boolean graphBaseContains(Triple t) {
		QueryPattern pattern = new QueryPattern(connection, graph, t);
		if (pattern.needsFilter()) {
			return containsByFind(t);
		}
		String tableName = pattern.getTableName();
		StringBuilder query = new StringBuilder(String.format(
				"SELECT Subject,Predicate,Object,Graph FROM %s.%s",
				keyspace, tableName));
		try {
			query.append(pattern.getWhereClause(tableName));
		} catch (TException e) {
			LOG.error("Bad query", e);
			return false;
		}
		query.append(" LIMIT 1");

		LOG.debug(query);
		ResultSet rs = connection.getSession().execute(query.toString());
		return rs.one() != null;
	}

	@Override
	protected ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {
		QueryPattern pattern = new QueryPattern(connection, graph,
				triplePattern);
		String tableName = pattern.getTableName();
		StringBuilder query = new StringBuilder(String.format(
				"SELECT Subject,Predicate,Object,Graph FROM %s.%s",
				keyspace, tableName));
		try {
			query.append(pattern.getWhereClause(tableName));
		} catch (TException e) {
			LOG.error("Bad query", e);
			return NiceIterator.emptyIterator();
		}

		LOG.debug(query);
		ResultSet rs = connection.getSession().execute(query.toString());
		ExtendedIterator<Quad> iter = WrappedIterator.create(rs.iterator())
				.mapWith(new RowToQuad()).filterDrop(new FindNull<Quad>());

		if (pattern.needsFilter()) {
			iter = iter.filterKeep(pattern.getQueryFilter());
		}
		return iter.mapWith(new QuadToTriple());
	}

	/**
	 * Function to convert quads to triples.
	 *
	 */
	private class QuadToTriple implements Function<Quad, Triple> {
		@Override
		public Triple apply(Quad t) {
			return t.asTriple();
		}
	}

}
