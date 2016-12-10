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

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.cassandra.graph.QueryPattern.FindNull;
import org.apache.jena.cassandra.graph.QueryPattern.RowToNode;
import org.apache.jena.cassandra.graph.QueryPattern.RowToQuad;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphUtil;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.core.DatasetGraphBase;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.apache.thrift.TException;

import com.datastax.driver.core.ResultSet;

/**
 * A Cassandra based DatasetGraph implementation.
 *
 */
public class DatasetGraphCassandra extends DatasetGraphBase {
	/*
	 * The Cassandra connection.
	 */
	private final CassandraConnection connection;
	/*
	 * The keyspace for to query.
	 */
	private final String keyspace;
	/*
	 * The name of the default graph.
	 */
	private Node defaultGraph;
	/*
	 * The graph table ID. This is used for finding the graph names.
	 */
	private static final String GRAPH_TABLE = "___g";

	private static final Log LOG = LogFactory.getLog(DatasetGraphCassandra.class);

	/**
	 * Constructor.
	 * 
	 * @param keyspace
	 *            The Cassandra keyspace to query.
	 * @param connection
	 *            the Cassandra connection.
	 */
	public DatasetGraphCassandra(String keyspace, CassandraConnection connection) {
		this.connection = connection;
		this.keyspace = keyspace;
		defaultGraph = Quad.defaultGraphIRI;
	}

	@Override
	public Iterator<Node> listGraphNodes() {
		String query = String.format("SELECT %1$s FROM %s.%s where token(%1$s)>%s", 
				ColumnName.G,
				keyspace, CassandraConnection.getTable(GRAPH_TABLE), Long.MIN_VALUE);
		LOG.debug(query);
		ResultSet rs = connection.getSession().execute(query);
		return WrappedIterator.create(rs.iterator()).mapWith(new RowToNode()).filterDrop(new FindNull<Node>()).toSet()
				.iterator();
	}

	private StringBuilder getSelect(QueryPattern pattern) throws TException {
		TableName tableName = pattern.getTableName();
		StringBuilder query = new StringBuilder(String.format("SELECT %s,%s,%s,%s FROM %s.%s",
				ColumnName.S, ColumnName.P, ColumnName.O,
				ColumnName.G, keyspace, tableName));

		query.append(pattern.getWhereClause(tableName));
		return query;
	}

	@Override
	public boolean contains(Node g, Node s, Node p, Node o) {
		QueryPattern pattern = new QueryPattern( g, Triple.createMatch(s, p, o));
		if (pattern.needsFilter()) {
			return super.contains(g, s, p, o);
		}
		try {
			StringBuilder query = getSelect(pattern);
			query.append(" LIMIT 1");
			LOG.debug(query);
			ResultSet rs = connection.getSession().execute(query.toString());
			return rs.one() != null;
		} catch (TException e) {
			LOG.error("Bad query", e);
			return false;
		}
	}

	@Override
	public Iterator<Quad> find(Node g, Node s, Node p, Node o) {

		QueryPattern pattern = new QueryPattern( g, Triple.createMatch(s, p, o));
		try {
			StringBuilder query = getSelect(pattern);
			LOG.debug(query);
			ResultSet rs = connection.getSession().execute(query.toString());
			ExtendedIterator<Quad> iter = WrappedIterator.create(rs.iterator()).mapWith(new RowToQuad())
					.filterDrop(new FindNull<Quad>());

			if (pattern.needsFilter()) {
				iter = iter.filterKeep(pattern.getQueryFilter());
			}
			return iter;
		} catch (TException e) {
			LOG.error("Bad query", e);
			return NiceIterator.emptyIterator();
		}

	}

	@Override
	public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
		QueryPattern pattern = new QueryPattern( g, Triple.createMatch(s, p, o));
		TableName tableName = pattern.getTableName();
		StringBuilder query = new StringBuilder(String.format("SELECT %s,%s,%s,%s FROM %s.%s",
				ColumnName.S, ColumnName.P, ColumnName.O,
				ColumnName.G, keyspace, tableName));
		StringBuilder whereClause = null;
		try {
			whereClause = pattern.getWhereClause(tableName);
			whereClause.append(whereClause.length() == 0 ? " WHERE " : " AND ").append("graph <> ")
					.append(pattern.valueOf(Quad.defaultGraphIRI));
		} catch (TException e) {
			LOG.error("Bad query", e);
			return NiceIterator.emptyIterator();
		}

		query.append(whereClause);
		LOG.debug(query);
		ResultSet rs = connection.getSession().execute(query.toString());
		ExtendedIterator<Quad> iter = WrappedIterator.create(rs.iterator()).mapWith(new RowToQuad())
				.filterDrop(new FindNull<Quad>());

		if (pattern.needsFilter()) {
			iter = iter.filterKeep(pattern.getQueryFilter());
		}
		return iter;
	}

	@Override
	public boolean supportsTransactions() {
		return false;
	}

	@Override
	public void begin(ReadWrite readWrite) {
		// do nothing

	}

	@Override
	public void commit() {
		// do nothing
	}

	@Override
	public void abort() {
		// do nothing
	}

	@Override
	public boolean isInTransaction() {
		return false;
	}

	@Override
	public void end() {
		// do nothing
	}

	@Override
	public Graph getDefaultGraph() {
		return getGraph(defaultGraph);
	}

	@Override
	public void setDefaultGraph(Graph g) {
		if (g instanceof GraphCassandra) {
			defaultGraph = ((GraphCassandra) g).getGraphName();
		} else {
			throw new IllegalArgumentException("Graph must a graph returned from getGraph()");
		}
	}

	@Override
	public Graph getGraph(Node graphNode) {
		return new GraphCassandra(graphNode, keyspace, connection);
	}

	@Override
	public void addGraph(Node graphName, Graph graph) {
		GraphCassandra graphImpl = (GraphCassandra) getGraph(graphName);
		graphImpl.clear();
		GraphUtil.addInto(graphImpl, graph);
	}

	@Override
	public void removeGraph(Node graphName) {
		GraphCassandra graphImpl = (GraphCassandra) getGraph(graphName);
		graphImpl.clear();
	}

	@Override
	public void add(Quad quad) {
		Graph g = getGraph(quad.getGraph());
		g.add(quad.asTriple());
	}

	@Override
	public void delete(Quad quad) {
		Graph g = getGraph(quad.getGraph());
		g.delete(quad.asTriple());
	}

	@Override
	public boolean isEmpty() {
		return getGraph(null).isEmpty();
	}

	@Override
	public long size() {
		QueryPattern pattern = new QueryPattern( null, Triple.ANY);
		TableName tableName = pattern.getTableName();
		ColumnName columnName = tableName.getPartitionKey();
		StringBuilder whereClause = null;
		try {
			whereClause = pattern.getWhereClause(tableName);
		} catch (TException e) {
			LOG.error("Error building where clause", e);
			whereClause = new StringBuilder();
		}
		whereClause.append(whereClause.length() == 0 ? " WHERE " : " AND ")
				.append(String.format("token(%s)>%s", columnName, Long.MIN_VALUE));
		String query = String.format("SELECT count(*) FROM %s.%s %s", columnName, keyspace, tableName, whereClause);
		LOG.debug(query);
		ResultSet rs = connection.getSession().execute(query);
		return rs.one().getLong(0);
	}

	@Override
	public void deleteAny(Node g, Node s, Node p, Node o) {
		if (g == null || Node.ANY.equals(g)) {
			Iterator<Node> graphs = listGraphNodes();
			while (graphs.hasNext()) {
				deleteAny(graphs.next(), s, p, o);
			}
			return;
		}
		getGraph(g).delete(Triple.createMatch(s, p, o));
	}
}
