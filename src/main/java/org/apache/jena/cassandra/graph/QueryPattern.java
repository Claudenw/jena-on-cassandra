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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.impl.XSDBaseNumericType;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.thrift.ThriftConvert;
import org.apache.jena.riot.thrift.wire.RDF_Term;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.mem.HexTable;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;

/**
 * Class that builds a query based on the graph Node and the triple pattern or a
 * Quad.
 *
 */
public class QueryPattern {

	/*
	 * We always select the columns from cassandra in the same order. Here they
	 * are defined as a string to be inserted into the select statement.
	 */
	private final static String SELECT_COLUMNS;

	static {
		ColumnName[] cols = new ColumnName[6];
		for (ColumnName c : ColumnName.values()) {
			if (c.getQueryPos() != -1) {
				cols[c.getQueryPos()] = c;
			}

		}
		StringBuilder sb = new StringBuilder();
		for (ColumnName c : cols) {
			if (sb.length() > 0) {
				sb.append(", ");
			}
			sb.append(c);

		}
		SELECT_COLUMNS = sb.toString();
	}

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

	/**
	 * Get the quad the pattern is working with.
	 * 
	 * @return the quad the pattern is working with.
	 */
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
	 * Get a list of query values for the columns in the order specified in the
	 * column names list. These are only the basic column not the extra columns
	 * 
	 * @param colNames
	 *            The column names to get the values for.
	 * @return the query data in order.
	 */
	public List<String> getQueryValues(List<ColumnName> colNames) {
		return getQueryValues(colNames, quad);
	}


	
	/**
	 * Get a list of query values for the columns in the order specified in the
	 * column names list.
	 * 
	 * @param colNames
	 *            The column names to get the values for.
	 * @param quad
	 *            The quad to extract the data from.
	 * @return
	 */
	public List<String> getQueryValues(List<ColumnName> colNames, Quad quad) {
		List<String> retval = new ArrayList<String>();
		for (ColumnName colName : colNames) {
			Node n = colName.getMatch(quad);
			if (n == null) {
				retval.add(null);
			} else {

				try {
					switch (colName) {
					case S:
					case P:
					case G:
						retval.add(valueOf(n));
						break;
					case O:
						retval.add((n.isLiteral() ? hexOf(n.getLiteralLexicalForm()) : valueOf(n)));
						break;
					case I:
						if (n.isLiteral() && n.getLiteralDatatype() instanceof XSDBaseNumericType) {
							retval.add(n.getLiteral().getLexicalForm());
						} else {
							retval.add(null);
						}
						break;
					case L:
						if (n.isLiteral() && StringUtils.isNotEmpty(n.getLiteralLanguage())) {
							retval.add(n.getLiteralLanguage());
						} else {
							retval.add(null);
						}
						break;
					case D:
						if (n.isLiteral()) {
							retval.add(n.getLiteralDatatypeURI());
						} else {
							retval.add(null);
						}
						break;
					default:
						retval.add(null);
					}

				} catch (TException e) {
					retval.add(null);
				}
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

	/**
	 * get the hex value for a string.
	 * 
	 * @param strValue
	 *            the string to convert
	 * @return The hex value string representing the input string.
	 */
	public String hexOf(String strValue) {
		return Bytes.toHexString(strValue.getBytes());
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
	private StringBuilder getWhereClause(TableName tableName) throws TException {
		QueryInfo queryInfo = new QueryInfo(quad);
		queryInfo.tableName = tableName;
		queryInfo.tableQuad = quad;
		return queryInfo.getWhereClause();
	}

	/**
	 * Execute a find on the database.
	 * 
	 * @param connection
	 *            The connection to use
	 * @param keyspace
	 *            The keyspace to query.
	 * @return An ExtendedIterator over the quads.
	 */
	public ExtendedIterator<Quad> doFind(CassandraConnection connection, String keyspace) {
		return doFind(connection, keyspace, null);
	}

	/**
	 * Execute a find on the database.
	 * 
	 * @param connection
	 *            The connection to use
	 * @param keyspace
	 *            The keyspace to query.
	 * @param extraWhere
	 *            Any extra clauses to add to the query. May be null.
	 * @return An ExtendedIterator over the quads.
	 */
	public ExtendedIterator<Quad> doFind(CassandraConnection connection, String keyspace, String extraWhere) {
		return doFind(connection, keyspace, extraWhere, null);
	}

	/**
	 * Execute a find on the database.
	 * 
	 * @param connection
	 *            The connection to use
	 * @param keyspace
	 *            The keyspace to query.
	 * @param extraWhere
	 *            Any extra clauses to add to the query. May be null.
	 * @param suffix
	 *            Any extra suffix to add to the query (e.g. "Limit 1"). May be
	 *            null.
	 * @return An ExtendedIterator over the quads.
	 */
	public ExtendedIterator<Quad> doFind(CassandraConnection connection, String keyspace, String extraWhere,
			String suffix) {
		try {
			QueryInfo queryInfo = new QueryInfo(quad);
			queryInfo.extraWhere = extraWhere;
			queryInfo.suffix = suffix;
			/* we always remove the Language from the where clause in a query as we 
			 * handle that below.  If the index type is included in the extra values then
			 * we also remove the data type from the where clause so we can match byte, int and long 
			 * as the same value.
			 */			
			if (queryInfo.extraValueMap.containsKey(ColumnName.I)) {
				queryInfo.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D);
			}
			else
			{
				queryInfo.extraValueFilter = Arrays.asList(ColumnName.L);
			}
			String query = getFindQuery(keyspace, queryInfo);
			ResultSet rs = connection.executeQuery(query);
			ExtendedIterator<Quad> iter = WrappedIterator.create(rs.iterator()).mapWith(new RowToQuad())
					.filterDrop(new FindNull<Quad>());
			/*
			 * if the language was included in the original quad.object then we
			 * need to filter base on the language column. this can not be done
			 * in cassandra as it does not have a concept of case inspecific.
			 */
			if (queryInfo.extraValueMap.containsKey(ColumnName.L)) {
				iter = iter.filterKeep(new LanguageFilter(queryInfo.extraValueMap.get(ColumnName.L)));
			}
			if (needsFilter()) {
				iter = iter.filterKeep(getQueryFilter());
			}
			return iter;
		} catch (TException e) {
			LOG.error("Bad query", e);
			return NiceIterator.emptyIterator();
		}
	}

	/*
	 * package private for testing
	 */
	/* package private */ String getFindQuery(String keyspace, QueryInfo queryInfo) throws TException {
		/*
		 * Adjust the table name based on the presence of the index on the
		 * object field. If the object field is a number then we need to use the
		 * index to filter it.
		 */

		StringBuilder query = new StringBuilder(
				String.format("SELECT %s FROM %s.%s", SELECT_COLUMNS, keyspace, queryInfo.tableName));

		StringBuilder whereClause = queryInfo.getWhereClause();
		if (queryInfo.extraWhere != null) {
			whereClause.append((whereClause.length() > 0) ? " AND " : " WHERE ");
			whereClause.append(queryInfo.extraWhere);
		}
		query.append(whereClause);

		if (queryInfo.suffix != null) {
			query.append(" ").append(queryInfo.suffix);
		}

		if (!queryInfo.extraValueMap.isEmpty()) {
			if (queryInfo.suffix == null || !queryInfo.suffix.toLowerCase().contains("allow filtering")) {
				query.append(" ALLOW FILTERING");
			}
		}

		LOG.debug(query);
		return query.toString();
	}

	/**
	 * Checks to see if the quad exists in the database.
	 * 
	 * @param connection
	 *            The connection to use.
	 * @param keyspace
	 *            the keyspace to check
	 * @return True the quad is in the database, false otherwise.
	 */
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
		for (ColumnName col : tableName.getPrimaryKeyColumns()) {
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

	public void doDelete(CassandraConnection connection, String keyspace) {

		ExtendedIterator<Quad> iter = doFind(connection, keyspace);
		ConcurrentHashMap<Runnable, ResultSetFuture> map = new ConcurrentHashMap<>();
		ForkJoinPool executor = new ForkJoinPool(4);

		while (iter.hasNext()) {

			QueryInfo queryInfo = new QueryInfo(iter.next());
			try {
				StringBuilder sb = new StringBuilder("BEGIN BATCH").append(System.lineSeparator());
				for (TableName tableName : CassandraConnection.getTableList()) {
					queryInfo.tableName = tableName;
					String whereClause = queryInfo.getWhereClause().toString();
					String cmd = String.format("DELETE FROM %s.%s %s;%n", keyspace, queryInfo.tableName.getName(),
							whereClause);
					sb.append(cmd);
				}
				String query = sb.append("APPLY BATCH;").toString();
				Runnable runner = new Runnable() {
					@Override
					public void run() {
						LOG.debug("delete completed");
						map.remove(this);
					}
				};
				if (LOG.isDebugEnabled()) {
					LOG.debug("executing query: " + query);
				}
				ResultSetFuture rsf = connection.getSession().executeAsync(query);
				map.put(runner, rsf);
				rsf.addListener(runner, executor);
			} catch (TException e) {
				LOG.error(String.format("Error deleting %s", queryInfo.tableQuad), e);
			}

		}

		while (!map.isEmpty()) {
			Thread.yield();
		}

		executor.shutdown();

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
		ResultSet rs = connection.executeQuery(query);
		return rs.one().getLong(0);
	}

	/*
	 * package private for testing purposes
	 */
	/* package private */ String getInsertStatement(String keyspace) throws TException {
		if (ColumnName.S.getMatch(quad) == null || ColumnName.P.getMatch(quad) == null
				|| ColumnName.O.getMatch(quad) == null || ColumnName.G.getMatch(quad) == null) {
			throw new IllegalArgumentException(
					"Graph, subject, predicate and object must be specified for an insert: " + quad.toString());
		}

		QueryInfo queryInfo = new QueryInfo( quad );
		Node object = ColumnName.O.getMatch(quad);
//		Map<ColumnName, Object> extraValueMap = getObjectExtraValues(object);

		StringBuilder sb = new StringBuilder("BEGIN BATCH").append(System.lineSeparator());

		for (TableName tbl : CassandraConnection.getTableList()) {
			sb.append(String.format("INSERT INTO %s.%s (%s, %s, %s, %s", keyspace, tbl, ColumnName.S, ColumnName.P,
					ColumnName.O, ColumnName.G));
			for (ColumnName colName : queryInfo.extraValueMap.keySet()) {
				sb.append(String.format(", %s", colName));
			}

			sb.append(String.format(") VALUES ( %s, %s, %s, %s", valueOf(ColumnName.S.getMatch(quad)),
					valueOf(ColumnName.P.getMatch(quad)),
					(object.isLiteral() ? hexOf(object.getLiteral().getLexicalForm()) : valueOf(object)),
					valueOf(ColumnName.G.getMatch(quad))));
			for (ColumnName colName : queryInfo.extraValueMap.keySet()) {
				sb.append(String.format(", %s", queryInfo.extraValueMap.get(colName)));
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
		connection.executeQuery(getInsertStatement(keyspace));
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
		public Quad apply(Row row) {

			RDF_Term subj = new RDF_Term();
			RDF_Term pred = new RDF_Term();
			RDF_Term graph = new RDF_Term();
			Node obj = null;
			try {
				dser.deserialize(subj, row.getBytes(ColumnName.S.getQueryPos()).array());
				dser.deserialize(pred, row.getBytes(ColumnName.P.getQueryPos()).array());
				dser.deserialize(graph, row.getBytes(ColumnName.G.getQueryPos()).array());

				if (row.getString(ColumnName.D.getQueryPos()) == null) {
					// not a literal just read the node
					RDF_Term objTerm = new RDF_Term();
					dser.deserialize(objTerm, row.getBytes(ColumnName.O.getQueryPos()).array());
					obj = ThriftConvert.convert(objTerm);
				} else {
					String lex = new String(row.getBytes(ColumnName.O.getQueryPos()).array());
					String lang = row.getString(ColumnName.L.getQueryPos());
					String dTypeURL = row.getString(ColumnName.D.getQueryPos());
					RDFDatatype dType = TypeMapper.getInstance().getSafeTypeByName(dTypeURL);
					obj = NodeFactory.createLiteral(lex, lang, dType);
				}
				return new Quad(ThriftConvert.convert(graph), ThriftConvert.convert(subj), ThriftConvert.convert(pred),
						obj);
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

	/**
	 * Basic language filter.
	 * 
	 * Basic filtering compares basic language ranges to language tags. Each
	 * basic language range in the language priority list is considered in turn,
	 * according to priority. A language range matches a particular language tag
	 * if, in a case-insensitive comparison, it exactly equals the tag, or if it
	 * exactly equals a prefix of the tag such that the first character
	 * following the prefix is "-". For example, the language-range "de-de"
	 * (German as used in Germany) matches the language tag "de-DE-1996" (German
	 * as used in Germany, orthography of 1996), but not the language tags
	 * "de-Deva" (German as written in the Devanagari script) or "de-Latn-DE"
	 * (German, Latin script, as used in Germany).
	 * 
	 * The special range "*" in a language priority list matches any tag. A
	 * protocol that uses language ranges MAY specify additional rules about the
	 * semantics of "*"; for instance, HTTP/1.1 [RFC2616] specifies that the
	 * range "*" matches only languages not matched by any other range within an
	 * "Accept-Language" header.
	 * 
	 * Basic filtering is identical to the type of matching described in
	 * [RFC3066], Section 2.5 (Language-range).
	 *
	 */
	private static class LanguageFilter implements Predicate<Quad> {
		private static final String WILDCARD = "*";
		private static final String SPLIT_TOKEN = "-";
		private String[] tags;

		public LanguageFilter(Object o) {
			this.tags = o.toString().split( SPLIT_TOKEN );
		}

		@Override
		public boolean test(Quad t) {
			if (t.getObject().isLiteral())
			{
				return doFilter(t.getObject().getLiteralLanguage());
			}
			return false;
		}
		
		private boolean doFilter( String lang )
		{
			String[] other = lang.split( SPLIT_TOKEN );
			if (other.length < tags.length)
			{
				return false;
			}
			
			for (int i=0;i<tags.length;i++)
			{
				if ( ! tags[i].equals( WILDCARD ))
				{
					if ( ! tags[i].equalsIgnoreCase( other[i] ))
					{
						if ( ! other[i].equals( WILDCARD))
						{
							return false;
						}
					}
				}
			}
			return true;
		}

	}
	
	public class QueryInfo {
		/**
		 * The quad we used to generate the table name.
		 */
		Quad tableQuad;
		/**
		 * Any extra values for where clause or filtering.
		 */
		Map<ColumnName, Object> extraValueMap;
		/**
		 * The table name
		 */
		TableName tableName;
		/**
		 * Any extra text to add to the where clause.  Should be of the form
		 * 'A=B AND C=D'
		 */
		String extraWhere;
		/**
		 * Any suffix to add to the where clause (e.g. limit=5)
		 */
		String suffix;
		/**
		 * Columns to remove from the where clause.  These are columns that will have 
		 * data in the extraValuesMap but for which we do not want to generate x=y statements
		 * in the where clause.
		 */
		Collection<ColumnName> extraValueFilter;

		public QueryInfo(Quad quad) {
			Node object = ColumnName.O.getMatch(quad);
			extraValueMap = getObjectExtraValues(object);

			tableQuad = quad;
			if (extraValueMap.containsKey(ColumnName.I)) {
				tableQuad = new Quad(quad.getGraph(), quad.getSubject(), quad.getPredicate(), Node.ANY);
			}

			tableName = CassandraConnection.getTable(CassandraConnection.getId(tableQuad));
		}

		/**
		 * Builds the where clause for a query based on the table name, the quad
		 * we are looing for and any extra values.
		 * 
		 * @param tableName
		 *            The table name to build the query for.
		 * @param quad
		 *            the quad to locate.
		 * @param extraValueMap
		 *            Any extra values from the quad object.
		 * @return A string builder that contains the constructed where clause.
		 * @throws TException
		 *             on encoding error.
		 */
		public StringBuilder getWhereClause() throws TException {

			StringBuilder result = new StringBuilder(" WHERE ");

			// get the key values for the primary key
			List<String> values = getQueryValues(tableName.getPrimaryKeyColumns(), tableQuad);

			// find the last primary key column that has a value.
			int lastCol = values.size();
			for (int i = values.size() - 1; i > 0; i--) {
				if (values.get(i) == null) {
					lastCol = i;
				}
			}
			if (lastCol == 0) {
				// no primary key columns have a value so start with the scan
				// value
				result.append(tableName.getPartitionKey().getScanValue());
			} else {
				/*
				 * for each of the columns specified do put in the value or the
				 * scan statement.
				 */
				for (int i = 0; i < lastCol; i++) {
					if (i > 0) {
						result.append(" AND ");
					}
					if (values.get(i) == null) {
						result.append(tableName.getPrimaryKeyColumn(i).getScanValue());
					} else {
						result.append(String.format("%s=%s", tableName.getPrimaryKeyColumn(i), values.get(i)));
					}
				}
			}

			Collection<ColumnName> keys = extraValueMap.keySet();
			if (extraValueFilter != null) {

				keys = WrappedIterator.create(keys.iterator()).filterDrop(new Predicate<ColumnName>() {

					@Override
					public boolean test(ColumnName t) {
						return extraValueFilter.contains(t);
					}
				}).toList();
			}

			/* if there are any extra values for the other columns add them */
			for (ColumnName colName : keys) {
				result.append(String.format(" AND %s=%s", colName, extraValueMap.get(colName)));
			}

			return result;
		}

		/**
		 * Create a map of columns to extra values for the specified object node.
		 * The resulting map will only have the column names for extra values needed
		 * for the object node type.
		 * 
		 * @param object
		 *            The object node to process
		 * @return A map of extra values indexed by columnName. May be empty but not
		 *         null.
		 */
		private Map<ColumnName, Object> getObjectExtraValues(Node object) {
			// use treemap to ensure column names are always returned in the same
			// order.
			Map<ColumnName, Object> retval = new TreeMap<ColumnName, Object>();

			if (object != null && object.isLiteral()) {
				if (object.getLiteralDatatype() instanceof XSDBaseNumericType) {
					retval.put(ColumnName.I, new BigDecimal(object.getLiteral().getLexicalForm()));
				}
				retval.put(ColumnName.D, String.format("'%s'", object.getLiteralDatatypeURI()));
				String lang = object.getLiteralLanguage();
				if (!StringUtils.isBlank(lang)) {
					retval.put(ColumnName.L, String.format("'%s'", lang.toLowerCase()));
				}
			}
			return retval;
		}
		
		
	}
}