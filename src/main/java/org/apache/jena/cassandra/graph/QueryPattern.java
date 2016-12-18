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
		ColumnName[] cols = new ColumnName[ColumnName.values().length];
		for (ColumnName c : ColumnName.values()) {
			if (c.getQueryPos() != -1) {
				cols[c.getQueryPos()] = c;
			}

		}
		StringBuilder sb = new StringBuilder();
		for (ColumnName c : cols) {
			if (c == null)
			{
				break;
			}
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
	 * The connection to the cassandra db
	 */
	private final CassandraConnection connection;

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
	public QueryPattern(CassandraConnection connection, Node graph, Triple triplePattern) {
		this(connection, new Quad(graph, triplePattern));
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
	public QueryPattern(CassandraConnection connection, Quad quad) {
		this.quad = new Quad(normalizeGraph(quad.getGraph()), quad.asTriple());
		this.connection = connection;
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
	 * Get the query info for this QueryPattern
	 * @return the query info
	 */
	public QueryInfo getQueryInfo()
	{
		return new QueryInfo(quad);
	}

	/**
	 * Get the query info for the specified quad
	 * @param quad the quad to get the query info for.
	 * @return the query info
	 */
	public QueryInfo getQueryInfo(Quad quad)
	{
		return new QueryInfo(quad);
	}

	/**
	 * Get a list of query values for all known columns.
	 * 
	 * Column values are strings except for numeric column I which is a big int.
	 * 
	 * @param quad
	 *            The quad to extract the data from.
	 * @return the map of column name to data
	 */
	public Map<ColumnName,Object> getQueryValues(Quad quad) {
		
		Map<ColumnName, Object> retval = new TreeMap<ColumnName, Object>();

//		if (object != null && object.isLiteral()) {
//			if (object.getLiteralDatatype() instanceof XSDBaseNumericType) {
//				retval.put(ColumnName.I, new BigDecimal(object.getLiteral().getLexicalForm()));
//			}
//			retval.put(ColumnName.D, String.format("'%s'", object.getLiteralDatatypeURI()));
//			String lang = object.getLiteralLanguage();
//			if (!StringUtils.isBlank(lang)) {
//				retval.put(ColumnName.L, String.format("'%s'", lang.toLowerCase()));
//			}
//		}
		Node n = null;
		for (ColumnName colName : ColumnName.values()) {		
				try {
					switch (colName) {
					case S:
					case P:
					case G:
					case O:
						n = colName.getMatch(quad);
						if (n != null)
						{
							retval.put(colName,connection.valueOf(n));
						}
						break;
					case V:
						n = ColumnName.O.getMatch(quad);
						if (n != null)
						{
							if (n.isLiteral())
							{
								retval.put(colName, n.getLiteralLexicalForm());
							}
						}
						break;
					case I:
						n = colName.getMatch(quad);
						if (n != null)
						{
						if (n.isLiteral() && n.getLiteralDatatype() instanceof XSDBaseNumericType) {
							retval.put(colName, new BigDecimal(n.getLiteral().getLexicalForm()));
						}
						}
						break;
					case L:
						n = colName.getMatch(quad);
						if (n != null)
						{
						if (n.isLiteral() && StringUtils.isNotEmpty(n.getLiteralLanguage())) {
							retval.put(colName,n.getLiteralLanguage());
						}
						}
						break;
					case D:
						n = colName.getMatch(quad);
						if (n != null)
						{
						if (n.isLiteral()) {
							retval.put(colName,n.getLiteralDatatypeURI());
						}
						}
						break;
					default:
						LOG.warn( "Unhandled column type: "+colName);
					}

				} catch (TException e) {
					LOG.error( String.format( "Error encoding %s: %s", colName, n), e);
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
	public ExtendedIterator<Quad> doFind(String keyspace) {
		return doFind(keyspace, null);
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
	public ExtendedIterator<Quad> doFind(String keyspace, String extraWhere) {
		return doFind(keyspace, extraWhere, null);
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
	public ExtendedIterator<Quad> doFind(String keyspace, String extraWhere,
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
			if (queryInfo.values.containsKey(ColumnName.I)) {
				queryInfo.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
			}
			else
			{
				queryInfo.extraValueFilter = Arrays.asList(ColumnName.L);
			}
			
			/**
			 * If the object is a literal remove the object from query consideration leaving just the values 
			 * extract from it and change the tablename so that we query appropriately.
			 */
			if (queryInfo.objectIsLiteral()) {
				queryInfo.tableQuad = new Quad(quad.getGraph(), quad.getSubject(), quad.getPredicate(), Node.ANY);
				queryInfo.values.remove(ColumnName.O);
				queryInfo.tableName = CassandraConnection.getTable(CassandraConnection.getId(queryInfo.tableQuad));
			}
			String query = getFindQuery( keyspace, queryInfo);
			ResultSet rs = connection.executeQuery(query);
			ExtendedIterator<Quad> iter = WrappedIterator.create(rs.iterator()).mapWith(new RowToQuad())
					.filterDrop(new FindNull<Quad>());
			/*
			 * if the language was included in the original quad.object then we
			 * need to filter base on the language column. this can not be done
			 * in cassandra as it does not have a concept of case inspecific.
			 */
			if (queryInfo.values.containsKey(ColumnName.L)) {
				iter = iter.filterKeep(new LanguageFilter(queryInfo.values.get(ColumnName.L)));
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

		
		if ( queryInfo.hasNonPrimaryData() ) {
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
	 * @param keyspace
	 *            the keyspace to check
	 * @return True the quad is in the database, false otherwise.
	 */
	public boolean doContains(String keyspace) {
		ExtendedIterator<Quad> iter = null;
		if (needsFilter()) {
			iter = doFind( keyspace);
		} else {
			iter = doFind( keyspace, null, "LIMIT 1");
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
	
	/* package private */ String getDeleteStatement( String keyspace, Quad quad ) throws TException
	{
		if (ColumnName.S.getMatch(quad) == null || ColumnName.P.getMatch(quad) == null
				|| ColumnName.O.getMatch(quad) == null || ColumnName.G.getMatch(quad) == null) {
			throw new IllegalArgumentException(
					"Graph, subject, predicate and object must be specified for a delete: " + quad.toString());
		}
		QueryInfo queryInfo = new QueryInfo(quad);
		queryInfo.extraValueFilter = queryInfo.getNonKeyColumns();
		
			StringBuilder sb = new StringBuilder("BEGIN BATCH").append(System.lineSeparator());
			for (TableName tableName : CassandraConnection.getTableList()) {
				queryInfo.tableName = tableName;
				String whereClause = queryInfo.getWhereClause().toString();
				String cmd = String.format("DELETE FROM %s.%s %s;%n", keyspace, queryInfo.tableName.getName(),
						whereClause);
				sb.append(cmd);
			}
			return sb.append("APPLY BATCH;").toString();
	}
	/**
	 * Delete the row(s) from the database.
	 * @param connection the cassandra connection to use.
	 * @param keyspace The keyspace to delete from.
	 */
	public void doDelete(String keyspace) {

		ExtendedIterator<Quad> iter = doFind( keyspace);
		ConcurrentHashMap<Runnable, ResultSetFuture> map = new ConcurrentHashMap<>();
		ForkJoinPool executor = new ForkJoinPool(4);

		while (iter.hasNext()) {

			try {
				Quad quad = iter.next();
				String query = getDeleteStatement( keyspace, quad );
			
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
				LOG.error(String.format("Error deleting %s", quad), e);
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
	public long getCount(String keyspace) throws TException {
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

		StringBuilder sb = new StringBuilder("BEGIN BATCH").append(System.lineSeparator());

		for (TableName tbl : CassandraConnection.getTableList()) {
			sb.append(String.format("INSERT INTO %s.%s (", keyspace, tbl));
			boolean first = true;
			for (ColumnName colName : queryInfo.values.keySet()) {
				if (!first) {
					sb.append( ", ");
				}
				sb.append(colName);
				first = false;
			}

			sb.append(") VALUES (");
			first = true;
			for (ColumnName colName : queryInfo.values.keySet()) {
				if (!first) {
					sb.append( ", ");
				}
				sb.append( colName.getInsertValue(queryInfo.values.get(colName)));
				first = false;
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
	public void doInsert(String keyspace) throws TException {
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
		
		Map<ColumnName,Object> values;

		private QueryInfo(Quad quad) {
			values = getQueryValues(quad);
			
			tableQuad = quad;

			tableName = CassandraConnection.getTable(CassandraConnection.getId(tableQuad));
						
		}

		/**
		 * Builds the where clause for a query based on the table name, the quad
		 * we are looing for and any extra values.

		 * @return A string builder that contains the constructed where clause.
		 * @throws TException
		 *             on encoding error.
		 */
		public StringBuilder getWhereClause() throws TException {


			// get the key values for the primary key
			if (values == null)
			{
				values = getQueryValues( tableQuad );
			}

			if (extraValueFilter != null)
			{
				for (ColumnName c : extraValueFilter)
				{
					values.remove(c);
				}
			}
			
			List<ColumnName> primaryKey = tableName.getPrimaryKeyColumns(); 
			/* find the last primary key column that has a value -- make the 
			 * object a null if it is not the only column that is not null. 
			 */
			int lastCol = -1;
			int i=0;
			for (ColumnName colName : primaryKey )
			{
				if (values.containsKey(colName))
				{
					if (colName != ColumnName.O)
					{												
						lastCol = i;
					} else {
						/* if the column is a literal skip it (and set the value to null)
						 and we will use the extra data to locate it. Otherwise
						 we just use the object column as it is a URI or anonymous
						 node */
						if ( objectIsLiteral())
						{
							values.remove( ColumnName.O);
						} else {							
							lastCol = i;						
						}
					}					
				}
				i++;
			}
			StringBuilder result = new StringBuilder(" WHERE ");			
			if (lastCol == -1) {
				// no primary key columns have a value so start with the scan
				// value
				result.append(tableName.getPartitionKey().getScanValue());
			} else {
				/*
				 * for each of the columns in the primary key put in the value or the
				 * scan statement.
				 */
				for (i=0;i<=lastCol;i++)
				{
					if (i>0)
					{
						result.append( " AND ");
					}
				
					ColumnName columnName = primaryKey.get(i);
					Object value = values.get(columnName);
					if (value == null)
					{
						result.append(columnName.getScanValue());
					}
					else
					{
						result.append(columnName.getEqualityValue( value));
					}
				}				
			}
			
			/* if the object is a literal put in the literal values */
			for (ColumnName colName : getNonKeyColumns())
			{
				
					Object value = values.get(colName);
					if (value != null)
					{
					result.append( " AND ").append( colName.getEqualityValue(value));
					}
				
			}

			return result;
		}
		
		public List<ColumnName> getNonKeyColumns()
		{
			List<ColumnName> lst = new ArrayList<ColumnName>( values.keySet());
			lst.removeAll( tableName.getPrimaryKeyColumns() );
			return lst;
		}

		public boolean hasNonPrimaryData()
		{
			return ! getNonKeyColumns().isEmpty();
		}
		
		public boolean objectIsLiteral()
		{
			return values.containsKey( ColumnName.V);
		}

//		/**
//		 * Create a map of columns to extra values for the specified object node.
//		 * The resulting map will only have the column names for extra values needed
//		 * for the object node type.
//		 * 
//		 * @param object
//		 *            The object node to process
//		 * @return A map of extra values indexed by columnName. May be empty but not
//		 *         null.
//		 */
//		private Map<ColumnName, Object> getObjectExtraValues(Node object) {
//			// use treemap to ensure column names are always returned in the same
//			// order.
//			Map<ColumnName, Object> retval = new TreeMap<ColumnName, Object>();
//
//			if (object != null && object.isLiteral()) {
//				if (object.getLiteralDatatype() instanceof XSDBaseNumericType) {
//					retval.put(ColumnName.I, new BigDecimal(object.getLiteral().getLexicalForm()));
//				}
//				retval.put(ColumnName.D, String.format("'%s'", object.getLiteralDatatypeURI()));
//				String lang = object.getLiteralLanguage();
//				if (!StringUtils.isBlank(lang)) {
//					retval.put(ColumnName.L, String.format("'%s'", lang.toLowerCase()));
//				}
//			}
//			return retval;
//		}
		
		
	}
}