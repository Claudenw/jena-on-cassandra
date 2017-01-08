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

import java.util.concurrent.ExecutionException;
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
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.sparql.core.Quad;
import org.apache.thrift.TException;

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
	 * 
	 * <ul>
	 * <li>A null graph name is considered as the default graph. e.g. urn:x-arq:DefaultGraph</li>
	 * <li>Node.ANY graph name is condiderd as the Union graph. e.g. urn:x-arq:UnionGraph</li>
	 * </ul>
	 * 
	 * @param graph
	 *            The name of the graph.
	 * @param keyspace
	 *            The keyspace to used.
	 * @param connection
	 *            The cassandra connection.
	 */
	public GraphCassandra(Node graph, String keyspace, CassandraConnection connection) {
		if (connection == null) {
			throw new IllegalArgumentException("Connection may not be null");
		}
		if (StringUtils.isBlank(keyspace)) {
			throw new IllegalArgumentException("Keyspace may not be null");
		}
		this.graph = graph == null ? Quad.defaultGraphIRI : (Quad.isUnionGraph(graph) ? Node.ANY : graph);
		this.connection = connection;
		this.keyspace = keyspace;
	}

	/**
	 * Constructor. Creates Union graph.
	 * 
	 * @param keyspace
	 *            The keyspace to used.
	 * @param connection
	 *            The cassandra connection.
	 */
	public GraphCassandra(String keyspace, CassandraConnection connection) {
		this(null, keyspace, connection);
	}

	/**
	 * Get the graph name. May be Node.ANY
	 * 
	 * @return The graph name.
	 */
	public Node getGraphName() {
		return graph;
	}

	@Override
	public void performAdd(Triple t) {

		QueryPattern pattern = new QueryPattern(connection, graph, t);
		try {
			pattern.doInsert(keyspace);
		} catch (TException e) {
			LOG.error("bad values", e);
		} catch (InterruptedException e) {
			LOG.error("Insert interrupted", e);
		} catch (ExecutionException e) {
			LOG.error("Insert error", e);
		}
	}

	@Override
	public void performDelete(Triple t) {

		// do not delete any triple with a wild card.
		if (t.getMatchSubject() == null || t.getMatchPredicate() == null ||
				t.getMatchObject() == null)
		{
			return;
		}

		QueryPattern pattern = new QueryPattern(connection, graph, t);
		pattern.doDelete(keyspace);

	}
	
	@Override
	public void remove(Node s, Node p, Node o)
	{
		if (Quad.isDefaultGraph(graph)) {
			throw new AddDeniedException("Can not delete from default graph named " + graph);
		}
		QueryPattern pattern = new QueryPattern(connection, graph, Triple.createMatch(s, p, o));
		pattern.doDelete(keyspace);
		getEventManager().notifyEvent(this, GraphEvents.remove(s, p, o) ) ;
	}

	@Override
	public void clear() {
		QueryPattern pattern = new QueryPattern( connection, graph, Triple.ANY );
		pattern.doDelete( keyspace );
		getEventManager().notifyEvent(this, GraphEvents.removeAll);
	}

	@Override
	public boolean isClosed() {
		return super.isClosed() || connection.getSession(keyspace).isClosed();
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
		return !pattern.doContains(keyspace);
	}

	@Override
	protected int graphBaseSize() {
		QueryPattern pattern = new QueryPattern(connection, graph, Triple.ANY);
		try {
			long retval = pattern.getCount(keyspace);
			return retval > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) retval;
		} catch (TException e) {
			LOG.error("Error building where clause", e);
			return -1;
		}
	}

	@Override
	protected boolean graphBaseContains(Triple t) {
		QueryPattern pattern = new QueryPattern(connection, graph, t);
		return pattern.doContains(keyspace);
	}

	@Override
	protected ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {
		QueryPattern pattern = new QueryPattern(connection, graph, triplePattern);
		return pattern.doFind(keyspace).mapWith(new QuadToTriple());
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
