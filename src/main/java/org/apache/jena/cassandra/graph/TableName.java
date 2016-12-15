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

import java.util.Arrays;
import java.util.List;

/**
 * Class to handle the table name in the Cassandra database. Table names are
 * comprised of the ColumnNames in a particular order. The column name order as
 * specified in the table name defiens the primary key.
 *
 */
public class TableName {
	/* The array of columns */
	private ColumnName cols[] = new ColumnName[4];
	/*
	 * The name of this table.
	 */
	private final String name;

	/**
	 * Constructor
	 * 
	 * @param name
	 *            The name the table.
	 */
	public TableName(String name) {
		this.name = name.toUpperCase();
		for (int i = 0; i < 4; i++) {
			cols[i] = ColumnName.valueOf(this.name.substring(i, i + 1));
		}
	}

	public int getColumnCount() {
		return 4;
	}

	/**
	 * Get the i'th column. 0<= i <= 3
	 * 
	 * @param i
	 *            the column number to get.
	 * @return the ColumnName at that position in the table definition.
	 */
	public ColumnName getColumn(int i) {
		if (i < 0 || i > 3) {
			throw new IndexOutOfBoundsException();
		}
		return cols[i];
	}

	/**
	 * Get the query columns for this table in order.
	 * 
	 * @return The list of Columns for this table in order.
	 */
	public List<ColumnName> getQueryColumns() {
		return Arrays.asList(cols);
	}

	@Override
	public String toString() {
		return name;
	}

	/**
	 * Get the name of this table.
	 * 
	 * @return This table name.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Build the primary key for a table.
	 * 
	 * This is the string used in the definition of the table and includes the
	 * parenthesis around the values.
	 * 
	 * @return the key definition for the table.
	 */
	private String getPrimaryKey() {
		StringBuilder sb = new StringBuilder("( ");
		for (ColumnName columnName : cols) {
			if (sb.length() > 2) {
				sb.append(", ");
			}
			sb.append(columnName);
		}
		return sb.append(" )").toString();
	}

	/**
	 * Get the Cassandra partition key for this table.
	 * 
	 * @return The column name for the partition key.
	 */
	public ColumnName getPartitionKey() {
		return cols[0];
	}

	/**
	 * Create the tables in the keyspace.
	 * 
	 * @param keyspace
	 *            The keyspace to create the tables in.
	 * @return an array of table and index creation strings.
	 */
	public String[] getCreateTableStatements(String keyspace) {
		String[] retval = new String[3];

		StringBuilder sb = new StringBuilder("CREATE TABLE ").append(String.format("%s.%s (", keyspace, this));
		for (ColumnName col : ColumnName.values()) {
			sb.append(col.getCreateText()).append(", ");
		}
		sb.append("PRIMARY KEY ").append(getPrimaryKey()).append(")");
		retval[0] = sb.toString();

		retval[1] = String.format("CREATE INDEX %2$s_%3$s ON %1$s.%2$s (%3$s)", keyspace, this, ColumnName.I);
		retval[2] = String.format("CREATE INDEX %2$s_%3$s ON %1$s.%2$s (%3$s)", keyspace, this, ColumnName.L);
		return retval;
	}
}
