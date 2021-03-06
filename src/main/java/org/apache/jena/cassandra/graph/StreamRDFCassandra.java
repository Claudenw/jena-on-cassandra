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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.apache.thrift.TException;

/**
 * An implementation of StreamRDF that writes to the Cassandra database.
 * 
 * This implementation uses the BulkExecutor to execute the the inserts.
 * 
 * Triples are loaded into the default graph.
 *
 */
public class StreamRDFCassandra implements StreamRDF {
	private CassandraConnection connection;
	private BulkExecutor bulkExecutor;
	private Log log;

	/**
	 * Constructor.
	 * 
	 * @param connection
	 *            The CassandraConnection to use.
	 * @param keyspace
	 *            The keyspace to use.
	 */
	public StreamRDFCassandra(CassandraConnection connection, String keyspace) {
		this.connection = connection;
		this.bulkExecutor = new BulkExecutor(connection.getSession(keyspace));
		this.log = LogFactory.getLog(StreamRDFCassandra.class.getName() + "." + hashCode());
		this.bulkExecutor.setLog(log);
	}

	@Override
	public void start() {
	}

	@Override
	public void triple(Triple triple) {
		quad(new Quad(Quad.defaultGraphIRI, triple));
	}

	@Override
	public void quad(Quad quad) {
		QueryPattern pattern = new QueryPattern(connection, quad);
		try {
			bulkExecutor.execute(pattern.getInsertStatement());
		} catch (TException e) {
			log.error(String.format("Unable to insert %s", quad), e);
		}
	}

	@Override
	public void base(String base) {
		// do nothing

	}

	@Override
	public void prefix(String prefix, String iri) {

	}

	@Override
	public void finish() {
		bulkExecutor.awaitFinish();
	}

}
