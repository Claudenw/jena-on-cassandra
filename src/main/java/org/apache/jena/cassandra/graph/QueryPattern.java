/*
 * 
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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.thrift.ThriftConvert;
import org.apache.jena.riot.thrift.wire.RDF_Term;
import org.apache.jena.sparql.core.Quad;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;

/**
 * Class that builds a query based on the graph Node and the triple pattern or a Quad. 
 *
 */
public class QueryPattern {
	/*
	 * The graph name 
	 */
	private final Node graph;
	/*
	 * The triple pattern to find
	 */
	private final Triple triplePattern;
	/*
	 * The connection to communicate with.
	 */
	private final CassandraConnection connection;
	/*
	 * A thrift serializer
	 */
	private TSerializer ser = new TSerializer();

	/**
	 * Constructor.
	 * @param connection The connection to use.
	 * @param graph The graph name
	 * @param triplePattern The triple to find.
	 */
	public QueryPattern(CassandraConnection connection, Node graph,
			Triple triplePattern) {
		this.graph = normalizeGraph(graph);
		this.triplePattern = triplePattern;
		this.connection = connection;
	}
	
	/**
	 * Normalize the  graph node to null for union graph or Node.ANY
	 * @param graph The graph to normalize
	 * @return The normalized graph.
	 */
	private static Node normalizeGraph( Node graph )
	{
		return (Node.ANY.equals(graph) || Quad.isUnionGraph(graph))? null : graph;
	}

	/**
	 * Constructor.
	 * @param connection The connection ot use.
	 * @param quad The quad to find.
	 */
	public QueryPattern(CassandraConnection connection, Quad quad) {
		this(connection, quad.getGraph(), quad.asTriple());
	}

	/**
	 * Get the table ID for this query.
	 * @return The table ID for this query.
	 */
	public String getId() {
		return CassandraConnection.getId(graph, triplePattern);
	}
	
	/**
	 * Get the table name for this query.
	 * @return The table we are going to query.
	 */
	public String getTableName() {
		return connection.getTable(getId());
	}

	/**
	 * Get the node for the match type.
	 * 
	 * A column name is one of "subject", "predicate", "object" or "graph".
	 * 
	 * Will not return Node.ANY.
	 * 
	 * @param columnName The column name to find.
	 * @return the value or null.  
	 */
	private Node getMatch(String columnName) {
		if (CassandraConnection.COL_SUBJECT.equals(columnName)) {
			return triplePattern.getMatchSubject();
		}
		if (CassandraConnection.COL_PREDICATE.equals(columnName)) {
			return triplePattern.getMatchPredicate();
		}
		if (CassandraConnection.COL_OBJECT.equals(columnName)) {
			return triplePattern.getMatchObject();
		}
		if (CassandraConnection.COL_GRAPH.equals(columnName)) {
			return this.graph;
		}
		throw new IllegalArgumentException(String.format(
				"column name %s is invalid", columnName));

	}

	/**
	 * Get the list of query column names in order specified by the name.
	 * 
	 * A column name is "subject", "predicate", "object" or "graph".
	 * 
	 * @param tblName The table name
	 * @return The list of column types.
	 */
	private List<String> getQueryColumns(String tblName) {
		List<String> retval = new ArrayList<String>();
		for (int i = 0; i < tblName.length(); i++) {
			char c = tblName.charAt(i);
			switch (c) {
			case 'S':
				retval.add(CassandraConnection.COL_SUBJECT);
				break;
			case 'P':
				retval.add(CassandraConnection.COL_PREDICATE);
				break;
			case 'O':
				retval.add(CassandraConnection.COL_OBJECT);
				break;
			case 'G':
				retval.add(CassandraConnection.COL_GRAPH);
				break;
			default:
				throw new IllegalArgumentException(String.format(
						"Table name %s is invalid at character %s", tblName, c));
			}
		}
		return retval;

	}

	/**
	 * Get the Cassandra partition key column type.
	 * @return The column name for the partition key.
	 */
	public String getPartitionKey() {
		return getQueryColumns(getTableName()).get(0);
	}

	/**
	 * Get the query filter used to filter results if needed.
	 * @return the QueryFilter.
	 */
	public Predicate<Quad> getQueryFilter() {
		return new ResultFilter(new Quad(this.graph, triplePattern));
	}

	/**
	 * Get a where clause segment. 
	 * @param needsAnd true if AND is required.
	 * @param sb the string builder we are adding to
	 * @param column the column name
	 * @param value the value (may be null).
	 * @return the value of needsAnd for the next whereClause call.
	 * @throws TException On serialization error.
	 */
	private boolean whereClause(boolean needsAnd, StringBuilder sb,
			String column, Node value) throws TException {
		if (value != null) {
			sb.append(needsAnd ? " AND " : " WHERE ").append(
					String.format("%s=%s", column, valueOf(value)));
			return true;
		}
		return needsAnd;
	}

	/**
	 * Get the values for a Cassandra insert statement.
	 * These are values that come after the "VALUES" keyword in the cassandra insert
	 * statement.  e.g everything within and including the parens in " VALUES ( s, p, o, g )"
	 * @return The values as a string for the cassandra insert statement.
	 * @throws TException on serialization error
	 */
	public String getValues() throws TException {
		return String.format("( %s, %s, %s, %s )",
				valueOf(triplePattern.getSubject()),
				valueOf(triplePattern.getPredicate()),
				valueOf(triplePattern.getObject()), valueOf(this.graph));
	}

	/**
	 * Return the serialized value of the node.
	 * @param node the node to serialize.
	 * @return The serialized node.
	 * @throws TException on serialization error.
	 */
	public String valueOf(Node node) throws TException {
		RDF_Term term = new RDF_Term();
		ThriftConvert.toThrift(node, null, term, false);
		byte[] bary = ser.serialize(term);
		return Bytes.toHexString(bary);
	}

	/**
	 * Get the where clause for the table.
	 * The where clause for the table must not skip any column
	 * names in the key, so this method returns a clause that only includes
	 * the contiguous segments from the key.
	 * @param tableName The table to create a where clause for.
	 * @return the where clause as a string builder.
	 * @throws TException on serialization error.
	 */
	public StringBuilder getWhereClause(String tableName) throws TException {
		boolean needsAnd = false;
		StringBuilder sb = new StringBuilder();
		for (String col : getQueryColumns(tableName)) {
			Node n = getMatch(col);
			if (n == null) {
				break;
			}
			needsAnd = whereClause(needsAnd, sb, col, getMatch(col));
		}
		return sb;
	}

	/**
	 * Get the where clause for the graph specified in the constructor.
	 * The where clause for the table must not skip any column
	 * names in the key, so this method returns a clause that only includes
	 * the contiguous segments from the key.
	 *  @return The where clause.
	 * @throws TException on serialization error.
	 */
	public StringBuilder getWhereClause() throws TException {
		return getWhereClause(getTableName());
	}

	/**
	 * Returns true if the resulting query needs a filter to return the proper values.
	 * @return true if the query needs a filter.
	 */
	public boolean needsFilter() {
		return connection.needsFilter(getId());
	}

	/**
	 * Returns true if the table name query needs a filter.
	 * @param tableName the table name to check.
	 * @return true if the table name needs a filter.
	 */
	public boolean needsFilter(String tableName) {
		boolean hasMissing = false;
		for (String col : getQueryColumns(tableName)) {
			Node n = getMatch(col);
			if (hasMissing && n != null) {
				return true;
			}
			hasMissing |= n != null;
		}
		return false;
	}


	/**
	 * A filter that ensures tha the results match the graph name and triple pattern for this
	 * query pattern.
	 *
	 */
	public class ResultFilter implements Predicate<Quad> {
		private Quad pattern;

		public ResultFilter(Quad pattern) {
			this.pattern = pattern;
		}

		@Override
		public boolean test(Quad t) {
			return t.matches(pattern.getGraph(), pattern.getSubject(),
					pattern.getPredicate(), pattern.getObject());
		}

	}

	/**
	 * Converts a Cassandra Row to a Quad.
	 *
	 */
	public static class RowToQuad implements Function<Row, Quad> {

		private TDeserializer dser = new TDeserializer();

		@Override
		public Quad apply(Row t) {

			RDF_Term subj = new RDF_Term();
			RDF_Term pred = new RDF_Term();
			RDF_Term obj = new RDF_Term();
			RDF_Term graph = new RDF_Term();
			try {
				dser.deserialize(subj, t.getBytes(0).array());
				dser.deserialize(pred, t.getBytes(1).array());
				dser.deserialize(obj, t.getBytes(2).array());
				dser.deserialize(graph, t.getBytes(3).array());
				return new Quad(ThriftConvert.convert(graph),
						ThriftConvert.convert(subj),
						ThriftConvert.convert(pred), ThriftConvert.convert(obj));
			} catch (TException e) {
				return null;
			}

		}
	}

	/**
	 * Converts a Cassandra Row to a Node.
	 *
	 */
	public static class RowToNode implements Function<Row, Node> {

		private TDeserializer dser = new TDeserializer();

		@Override
		public Node apply(Row t) {

			RDF_Term node = new RDF_Term();
			try {
				dser.deserialize(node, t.getBytes(0).array());
				return ThriftConvert.convert(node);
			} catch (TException e) {
				return null;
			}
		}
	}

	/**
	 * A filter that finds Null values.
	 *
	 * @param <T> The type of object we are iterating over.
	 */
	public static class FindNull<T> implements Predicate<T> {
		@Override
		public boolean test(T t) {
			return t == null;
		}
	}
}