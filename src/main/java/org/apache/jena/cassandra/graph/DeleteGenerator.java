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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.jena.cassandra.graph.iterators.CascadingIterator;
import org.apache.jena.cassandra.graph.iterators.ColIterator;

/**
 * Class to generate delete statements for tables.
 * 
 * Generates all the necessary delete statements for a given keyspace.
 *
 */
public class DeleteGenerator extends CascadingIterator<TableName> {
	private final CassandraConnection connection;
	private final String keyspace;
	private final QueryPattern pattern;
	
	/**
	 * Constructor.
	 * @param connection The Cassandra connection.
	 * @param keyspace The keyspace to use.
	 * @param pattern The query pattern to delete.
	 */
	public DeleteGenerator( CassandraConnection connection, String keyspace, QueryPattern pattern )
	{
		this.connection = connection;
		this.keyspace = keyspace;
		this.pattern = pattern;
		setBaseIterator(CassandraConnection.getTableList().iterator());
	}
	
	@Override
	protected Iterator<String> createSubIter() {
		List<String> colValues = Collections.unmodifiableList(pattern.getQueryValues(thisValue.getPrimaryKeyColumns()));			
		return new ColIterator( connection, keyspace, thisValue, 0, colValues );
	}
	
	
	@Override
	public String next() {
		if (!hasNext())
		{
			throw new NoSuchElementException();	
		}
		return String.format( "DELETE FROM %s.%s WHERE %s", keyspace, thisValue, subIter.next() );
	}

}
