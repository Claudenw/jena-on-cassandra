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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.datatypes.xsd.impl.XSDBaseNumericType;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.thrift.ThriftConvert;
import org.apache.jena.riot.thrift.wire.RDF_Term;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;

/**
 * Class that builds a query based on the graph Node and the triple pattern or a
 * Quad.
 *
 */
public class QueryPattern {
	/*
	 * The descriptive quad
	 */
	private final Quad quad;

	/*
	 * A thrift serializer
	 */
	private TSerializer ser = new TSerializer();

	private static final Log LOG = LogFactory.getLog(QueryPattern.class);

	/**
	 * Constructor.
	 * 
	 * @param connection
	 *            The connection to use.
	 * @param graph
	 *            The graph name
	 * @param triplePattern
	 *            The triple to find.
	 */
	public QueryPattern(Node graph, Triple triplePattern) {
		this(new Quad(graph, triplePattern));
	}

	/**
	 * Normalize the graph node to null for union graph or Node.ANY
	 * 
	 * @param graph
	 *            The graph to normalize
	 * @return The normalized graph.
	 */
	private static Node normalizeGraph(Node graph) {
		return (Node.ANY.equals(graph) || Quad.isUnionGraph(graph)) ? null : graph;
	}

	/**
	 * Constructor.
	 * 
	 * @param connection
	 *            The connection ot use.
	 * @param quad
	 *            The quad to find.
	 */
	public QueryPattern(Quad quad) {
		this.quad = new Quad(normalizeGraph(quad.getGraph()), quad.asTriple());

	}

	public Quad getQuad() {
		return quad;
	}

	/**
	 * Get the table ID for this query.
	 * 
	 * @return The table ID for this query.
	 */
	public String getId() {
		return CassandraConnection.getId(quad);
	}

	/**
	 * Get the table name for this query.
	 * 
	 * @return The table we are going to query.
	 */
	public TableName getTableName() {
		return CassandraConnection.getTable(getId());
	}

	/**
	 * Get the query values for the columns. The returned values are the
	 * serialized values of the data or null if the data was not specified.
	 * 
	 * @param colNames
	 *            The column names to get
	 * @return The query values in colNames order.
	 */
	public List<String> getQueryValues(List<ColumnName> colNames) {
		return getQueryValues( colNames, quad );
	}
	
	public List<String> getQueryValues(List<ColumnName> colNames, Quad quad) {
			List<String> retval = new ArrayList<String>();
		for (ColumnName colName : colNames) {
			Node n = colName.getMatch(quad);
			try {
				retval.add((n == null) ? null : valueOf(n));
			} catch (TException e) {
				retval.add(null);
			}
		}
		return retval;
	}

	/**
	 * Get the query filter used to filter results if needed.
	 * 
	 * @return the QueryFilter.
	 */
	public Predicate<Quad> getQueryFilter() {
		return new ResultFilter(quad);
	}

	/**
	 * Get the values for a Cassandra insert statement. These are values that
	 * come after the "VALUES" keyword in the cassandra insert statement. e.g
	 * everything within and including the parens in " VALUES ( s, p, o, g )"
	 * 
	 * @return The values as a string for the cassandra insert statement.
	 * @throws TException
	 *             on serialization error
	 */
	// public String getValues() throws TException {
	// return String.format("( %s, %s, %s, %s )",
	// valueOf(ColumnName.S.getMatch(quad)),
	// valueOf(ColumnName.P.getMatch(quad)),
	// valueOf(ColumnName.O.getMatch(quad)),
	// valueOf(ColumnName.G.getMatch(quad)));
	// }

	/**
	 * Return the serialized value of the node.
	 * 
	 * @param node
	 *            the node to serialize.
	 * @return The serialized node.
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
	 * Get the where clause for the table. The where clause for the table must
	 * not skip any column names in the key, so this method returns a clause
	 * that only includes the contiguous segments from the key.
	 * 
	 * @param tableName
	 *            The table to create a where clause for.
	 * @return the where clause as a string builder.
	 * @throws TException
	 *             on serialization error.
	 */
	public StringBuilder getWhereClause(TableName tableName) throws TException {
		return getWhereClause( tableName, quad );
	}
	
	public StringBuilder getWhereClause(TableName tableName, Quad quad) throws TException {
		
		StringBuilder result = new StringBuilder(" WHERE ");

		List<String> values = getQueryValues(tableName.getQueryColumns(), quad);
		int lastCol = values.size();
		for (int i = values.size() - 1; i > 0; i--) {
			if (values.get(i) == null) {
				lastCol = i;
			}
		}
		if (lastCol == 0) {
			return result.append(tableName.getPartitionKey().getScanValue());
		}
		for (int i = 0; i < lastCol; i++) {
			if (i > 0) {
				result.append(" AND ");
			}
			if (values.get(i) == null) {
				result.append(tableName.getColumn(i).getScanValue());
			} else {
				result.append(String.format("%s=%s", tableName.getColumn(i), values.get(i)));
			}
		}
		
		BigDecimal index = getObjectValue();
		if (index != null)
		{
			result.append( String.format(" AND %s=%s", ColumnName.I, index));
		}
		return result;
	}

	public ExtendedIterator<Quad> doFind(CassandraConnection connection, String keyspace) {
		return doFind(connection, keyspace, null);
	}

	public ExtendedIterator<Quad> doFind(CassandraConnection connection, String keyspace, String extraWhere) {
		return doFind(connection, keyspace, extraWhere, null);
	}

	public ExtendedIterator<Quad> doFind(CassandraConnection connection, String keyspace, String extraWhere,
			String suffix) {
		try {
			String query = getFindQuery( keyspace, extraWhere, suffix );
		ResultSet rs = connection.getSession().execute(query);
		ExtendedIterator<Quad> iter = WrappedIterator.create(rs.iterator()).mapWith(new RowToQuad())
				.filterDrop(new FindNull<Quad>());
		if (needsFilter()) {
			iter = iter.filterKeep(getQueryFilter());
		}
		return iter;
		} catch (TException e) {
			LOG.error("Bad query", e);
			return NiceIterator.emptyIterator();
		}
	}

	public String getFindQuery(String keyspace, String extraWhere,
			String suffix) throws TException
	{
		/*
		 * Adjust the table name based on the presence of the index on the object field.
		 * If the object field is a number then we need to use the index to filter it.
		 */
		BigDecimal index = getObjectValue();
		Quad tableQuad = quad;
		//QueryPattern tablePattern = this;
		if (index != null)
		{
			tableQuad = new Quad( quad.getGraph(), quad.getSubject(), quad.getPredicate(), Node.ANY );
			//tablePattern = new QueryPattern( tableQuad );
		}

		TableName tableName = CassandraConnection.getTable(CassandraConnection.getId(tableQuad));
		StringBuilder query = new StringBuilder(
				String.format("SELECT Subject,Predicate,Object,Graph FROM %s.%s", keyspace, tableName));
		
			StringBuilder whereClause = getWhereClause(tableName, tableQuad);
			if (extraWhere != null) {
				whereClause.append((whereClause.length() > 0) ? " AND " : " WHERE ");
				whereClause.append(extraWhere);
			}
			query.append(whereClause);
		
		if (suffix != null) {
			query.append(" ").append(suffix);
		}

		LOG.debug(query);	
		return query.toString();
	}
	
	public boolean doContains(CassandraConnection connection, String keyspace) {
		ExtendedIterator<Quad> iter = null;
		if (needsFilter()) {
			iter = doFind(connection, keyspace);
		} else {
			iter = doFind(connection, keyspace, null, "LIMIT 1");
		}
		try {
			return iter.hasNext();
		} finally {
			iter.close();
		}
	}

	/**
	 * Get the where clause for the graph specified in the constructor. The
	 * where clause for the table must not skip any column names in the key, so
	 * this method returns a clause that only includes the contiguous segments
	 * from the key.
	 * 
	 * @return The where clause.
	 * @throws TException
	 *             on serialization error.
	 */
	public StringBuilder getWhereClause() throws TException {
		return getWhereClause(getTableName());
	}

	/**
	 * Returns true if the resulting query needs a filter to return the proper
	 * values.
	 * 
	 * @return true if the query needs a filter.
	 */
	public boolean needsFilter() {
		return CassandraConnection.needsFilter(getId());
	}

	/**
	 * Returns true if the table query needs a filter.
	 * 
	 * @param tableName
	 *            the table name to check.
	 * @return true if the table name needs a filter.
	 */
	public boolean needsFilter(TableName tableName) {
		boolean hasMissing = false;
		for (ColumnName col : tableName.getQueryColumns()) {
			Node n = col.getMatch(quad);
			if (hasMissing && n != null) {
				return true;
			}
			hasMissing |= n == null;
		}
		return false;
	}

	/**
	 * Get an iterator of delete statements for the tables.
	 * 
	 * @param graph
	 *            The graph we are deleting from (may be null)
	 * @param pattern
	 *            THe pattern we are deleting from the graph. (may be
	 *            Triple.ANY)
	 * @return
	 */
	public Iterator<String> getDeleteStatements(CassandraConnection connection, String keyspace) {
		if (quad.getGraph() == null && Triple.ANY.equals(quad.asTriple())) {
			return WrappedIterator.create(CassandraConnection.getTableList().iterator())
					.mapWith(new Function<TableName, String>() {

						@Override
						public String apply(TableName arg0) {

							return String.format("TRUNCATE %s.%s", keyspace, arg0);
						}
					});
		}

		return new DeleteGenerator(connection, keyspace, this);
	}

	/**
	 * Get a count of the triples that match the pattern for this table in the
	 * specified keyspace.
	 * 
	 * @param connection
	 *            the Cassandra connection to use.
	 * @param keyspace
	 *            the keyspace to query.
	 * @return The count as a long
	 * @throws TException
	 *             On serialization error.
	 */
	public long getCount(CassandraConnection connection, String keyspace) throws TException {
		TableName tableName = getTableName();
		String query = String.format("SELECT count(*) FROM %s.%s %s", keyspace, tableName, getWhereClause(tableName));
		LOG.debug(query);
		ResultSet rs = connection.getSession().execute(query);
		return rs.one().getLong(0);
	}

	private BigDecimal getObjectValue()
	{
		Node object = ColumnName.O.getMatch(quad);
		BigDecimal index = null;
		if (object != null && object.isLiteral() && object.getLiteralDatatype() instanceof XSDBaseNumericType) {
			index = new BigDecimal(object.getLiteral().getLexicalForm());
		}
		return index;
	}
	
	public String getInsertStatement(String keyspace) throws TException
	{
		if (ColumnName.S.getMatch(quad) == null || ColumnName.P.getMatch(quad) == null
				|| ColumnName.O.getMatch(quad) == null || ColumnName.G.getMatch(quad) == null) {
			throw new IllegalArgumentException(
					"Graph, subject, predicate and object must be specified for an insert: " + quad.toString());
		}

		BigDecimal index = getObjectValue();

		StringBuilder sb = new StringBuilder("BEGIN BATCH").append(System.lineSeparator());

		for (TableName tbl : CassandraConnection.getTableList()) {
			sb.append(String.format("INSERT INTO %s.%s (%s, %s, %s, %s", keyspace, tbl, ColumnName.S, 
					ColumnName.P,
					ColumnName.O, ColumnName.G));
			if (index != null) {
				sb.append(String.format(", %s", ColumnName.I));
			}
			sb.append(String.format(") VALUES ( %s, %s, %s, %s", valueOf(ColumnName.S.getMatch(quad)),
					valueOf(ColumnName.P.getMatch(quad)), valueOf(ColumnName.O.getMatch(quad)),
					valueOf(ColumnName.G.getMatch(quad))));
			if (index != null) {
				sb.append(", ").append(index);
			}
			sb.append(");").append(System.lineSeparator());
		}
		return sb.append("APPLY BATCH;").toString();
	}
	
	/**
	 * Performs the insert of the data.
	 * 
	 * @param connection
	 *            The cassandra connection.
	 * @param keyspace
	 *            The keyspace for the table.
	 * @throws TException
	 *             on encoding error.
	 */
	public void doInsert(CassandraConnection connection, String keyspace) throws TException {
		String stmt = getInsertStatement(keyspace);
		LOG.debug(stmt);
		connection.getSession().execute(stmt);
	}

	/**
	 * A filter that ensures tha the results match the graph name and triple
	 * pattern for this query pattern.
	 *
	 */
	public class ResultFilter implements Predicate<Quad> {
		private Quad pattern;

		public ResultFilter(Quad pattern) {
			this.pattern = pattern;
		}

		@Override
		public boolean test(Quad t) {
			return t.matches(pattern.getGraph(), pattern.getSubject(), pattern.getPredicate(), pattern.getObject());
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
				return new Quad(ThriftConvert.convert(graph), ThriftConvert.convert(subj), ThriftConvert.convert(pred),
						ThriftConvert.convert(obj));
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
	 * @param <T>
	 *            The type of object we are iterating over.
	 */
	public static class FindNull<T> implements Predicate<T> {
		@Override
		public boolean test(T t) {
			return t == null;
		}
	}
}