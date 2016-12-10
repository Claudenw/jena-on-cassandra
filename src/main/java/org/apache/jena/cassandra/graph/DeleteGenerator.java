package org.apache.jena.cassandra.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.apache.jena.util.iterator.SingletonIterator;
import org.apache.jena.util.iterator.WrappedIterator;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;


public class DeleteGenerator implements Iterator<String> {
	private final CassandraConnection connection;
	private final String keyspace;
	private final QueryPattern pattern;
	private final Iterator<TableName> baseIter;
	private Iterator<String> subIter;
	
	public DeleteGenerator( CassandraConnection connection, String keyspace, QueryPattern pattern, Iterator<TableName> baseIter )
	{
		this.connection = connection;
		this.keyspace = keyspace;
		this.pattern = pattern;
		this.baseIter = baseIter;
		this.subIter = null;
	}
	
	private Iterator<String> nextSubIter() {
		TableName tableName = baseIter.next();
		List<String> colValues = Collections.unmodifiableList(pattern.getQueryValues(tableName.getQueryColumns()));			
		return WrappedIterator.create(new ColIterator( 0, colValues, tableName )).mapWith( new Function<String,String>(){

			@Override
			public String apply(String t) {
				return String.format( "DELETE FROM %s.%s WHERE %s", keyspace, tableName, t );
			}});		
	}

	@Override
	public boolean hasNext() {
		if (subIter != null)
		{
			if (subIter.hasNext())
			{
				return true;
			}
		}
		return baseIter.hasNext();
	}

	@Override
	public String next() {
		if (hasNext())
		{
			if (subIter == null || !subIter.hasNext())
			{
				subIter = nextSubIter();
			}
			return subIter.next();
		}
		throw new NoSuchElementException();
	}
	
	private Iterator<String> getColumnIterator(int pos, List<String> colValues, TableName tableName)
	{
		ColumnName reqColumn = tableName.getColumn(pos);
		
		StringBuilder queryStr = new StringBuilder(String.format("SELECT %s FROM %s.%s WHERE ",
				reqColumn,
				keyspace, tableName));
		if (pos == 0)
		{
			queryStr.append( String.format("token(%s) > %s", reqColumn, Integer.MIN_VALUE));
		} else {
			for (int i=0;i<pos;i++)
			{
				if (i>0)
				{
					queryStr.append( " AND ");
				}
				queryStr.append( String.format( "%s=%s", tableName.getColumn(i), colValues.get(i)));
			}
		}
		return	WrappedIterator.create(connection.getSession().execute(queryStr.toString()).iterator()).mapWith( new Function<Row,String>(){
				@Override
				public String apply(Row arg0) {
					return Bytes.toHexString(arg0.getBytes(0));
				}});
	}
	
	private class ColIterator implements Iterator<String> {
		private final TableName tableName;
		private final List<String> colValues; 
		private final ColumnName colName;
		private Iterator<String> baseIter;
		private final int pos;
		private ColIterator subIter;
		private String thisValue;
		private final boolean endIter;
		
		public ColIterator(int pos, List<String> colValues, TableName tableName )
		{
			this.tableName = tableName;
			this.pos=pos;
			this.colName = tableName.getColumn(pos);
			this.colValues = colValues;
			this.subIter = null;
			this.thisValue = null;
			if (colValues.get(pos) == null)
			{
				this.baseIter = getColumnIterator( pos, colValues, tableName );
			} else {
				this.baseIter = new SingletonIterator<String>( colValues.get(pos));
			}
			boolean end = true;
			for (int i=pos+1;i<4;i++)
			{
				if (colValues.get(i) != null)
				{
					end = false;
				}
			}
			endIter = end;
		}
		
		@Override
		public boolean hasNext() {
			if (subIter != null)
			{
				if (subIter.hasNext())
				{
					return true;
				}
			}
			return baseIter.hasNext();	
		}
		
		@Override
		public String next() {
			if (!hasNext())
			{
				throw new NoSuchElementException();	
			}
			if (thisValue == null || (subIter != null && !subIter.hasNext()))
			{
				thisValue = baseIter.next();
				if (!endIter)
				{
					ArrayList newValues = new ArrayList( colValues );
					newValues.set( pos, thisValue);
					this.subIter = new ColIterator(pos+1, newValues, tableName);
				}
			}
			StringBuilder sb = new StringBuilder();
			sb.append( String.format( "%s=%s", colName, thisValue ));
			if (subIter != null)
			{
				sb.append( " AND " ).append( subIter.next() );
			} else {
				thisValue = null;
			}
			return sb.toString();
		}
	}
		
	

}
