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
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphUtil;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.core.DatasetGraphBase;
import org.apache.jena.sparql.core.Quad;
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
		String query = String.format("SELECT %1$s FROM %s.%s where token(%1$s)>%s", ColumnName.G, keyspace,
				CassandraConnection.getTable(GRAPH_TABLE), Long.MIN_VALUE);
		LOG.debug(query);
		ResultSet rs = connection.getSession().execute(query);
		return WrappedIterator.create(rs.iterator()).mapWith(new RowToNode()).filterDrop(new FindNull<Node>()).toSet()
				.iterator();
	}

	@Override
	public boolean contains(Node g, Node s, Node p, Node o) {
		QueryPattern pattern = new QueryPattern(connection, g, Triple.createMatch(s, p, o));
		return pattern.doContains( keyspace);
	}

	@Override
	public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
		QueryPattern pattern = new QueryPattern(connection, g, Triple.createMatch(s, p, o));
		return pattern.doFind( keyspace);
	}

	@Override
	public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
		QueryPattern pattern = new QueryPattern(connection, g, Triple.createMatch(s, p, o));
		try {
			return pattern.doFind( keyspace, "graph <> " + connection.valueOf(Quad.defaultGraphIRI));
		} catch (TException e) {
			LOG.error("Unable to execute findNG", e);
			return NiceIterator.emptyIterator();
		}
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
		return getGraph( Quad.unionGraph ).isEmpty();
	}

	@Override
	public long size() {
		QueryPattern pattern = new QueryPattern(connection, null, Triple.ANY);
		try {
			return pattern.getCount(keyspace);
		} catch (TException e) {
			LOG.error("Error building where clause", e);
			return -1;
		}
	}

	@Override
	public void deleteAny(Node g, Node s, Node p, Node o) {
		Quad q = new Quad( g, s==null?Node.ANY:s, p==null?Node.ANY:p, o==null?Node.ANY:o );
		QueryPattern pattern = new QueryPattern(connection, q);	
		pattern.doDelete(keyspace);	
	}

}
