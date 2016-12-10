package org.apache.jena.cassandra.graph;

import java.util.Arrays;
import java.util.List;

public class TableName {
	private ColumnName cols[] = new ColumnName[4];
	private final String name;
	
	public TableName( String name )
	{
		this.name=name;
		for (int i=0;i<4;i++)
		{
			cols[i] = ColumnName.valueOf( name.substring(i,i+1));
		}
	}
	
	public ColumnName getColumn( int i )
	{
		if (i<0 || i>3)
		{
			throw new IndexOutOfBoundsException();
		}
		return cols[i];
	}
	
	public List<ColumnName> getQueryColumns() {
		return Arrays.asList(cols);
	}
	
	@Override
	public String toString()
	{
		return name;
	}
	
	public String getName()
	{
		return name;
	}
	
	/**
	 * Build the primary key for a table.
	 * 
	 * @param tblName
	 *            The table name
	 * @return the key definition for the table.
	 */
	public String getPrimaryKey() {
		StringBuilder sb = new StringBuilder("( ");
		for (ColumnName columnName : cols ) {
			if (sb.length() > 2) {
				sb.append(", ");
			}
			sb.append(columnName);
		}
		return sb.append(" )").toString();
	}
	

	/**
	 * Get the Cassandra partition key column type.
	 * @return The column name for the partition key.
	 */
	public ColumnName getPartitionKey() {
		return cols[0];
	}
}
