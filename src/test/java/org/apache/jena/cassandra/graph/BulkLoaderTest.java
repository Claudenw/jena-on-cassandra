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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.jena.cassandra.CassandraSetup;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BulkLoaderTest {

	private CassandraConnection connection;
	private static final String KEYSPACE = "test";
	private static CassandraSetup cassandra;

	/**
	 * Set embedded cassandra up and spawn it in a new thread.
	 *
	 * @throws TTransportException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@BeforeClass
	public static void before() throws Exception, InterruptedException {
		cassandra = new CassandraSetup();
	}

	@AfterClass
	public static void after() {
		cassandra.shutdown();
	}

	@Before
	public void setupTestGraphCassandra()
			throws ConfigurationException, TTransportException, IOException, InterruptedException {
		connection = new CassandraConnection(cassandra.getCluster(), cassandra.getNodeProbeConfig());
		connection.createKeyspace(String.format(
				"CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
				KEYSPACE));

		connection.createTables(KEYSPACE);
		connection.truncateTables(KEYSPACE);
	}

	private String getURLStr(String fn) {
		return Thread.currentThread().getContextClassLoader().getResource(fn).toString();
	}

	@Test
	public void testLoad() {
		List<String> urls = new ArrayList<String>();
		urls.add(getURLStr("bulkLoader/model0.nt"));
		urls.add(getURLStr("bulkLoader/model1.nt"));
		urls.add(getURLStr("bulkLoader/model2.nt"));
		urls.add(getURLStr("bulkLoader/model3.nt"));
		urls.add(getURLStr("bulkLoader/model4.nt"));
		urls.add(getURLStr("bulkLoader/model5.nt"));
		urls.add(getURLStr("bulkLoader/model5.rdf"));
		urls.add(getURLStr("bulkLoader/model6.nt"));
		urls.add(getURLStr("bulkLoader/model7.nt"));
		urls.add(getURLStr("bulkLoader/model8.n3"));
		urls.add(getURLStr("bulkLoader/model9.n3"));
		urls.add(getURLStr("bulkLoader/modelA.nt"));
		BulkLoader.execute(connection, KEYSPACE, urls);

		// <urn:xyz:abc> <http://localhost/p1> "/*Not a comment*/" .
		Triple first = new Triple(NodeFactory.createURI("urn:xyz:abc"), NodeFactory.createURI("http://localhost/p1"),
				NodeFactory.createLiteral("/*Not a comment*/"));

		// <http://rdf.hp.com/r-\u00E9> <http://rdf.hp.com/p1> "value" .

		Triple last = new Triple(NodeFactory.createURI("http://rdf.hp.com/r-\u00E9"),
				NodeFactory.createURI("http://rdf.hp.com/p1"), NodeFactory.createLiteral("value"));

		GraphCassandra graph = new GraphCassandra(Quad.defaultGraphIRI, KEYSPACE, connection);
		assertTrue(String.format("Should have contained %s", first), graph.contains(first));
		assertTrue(String.format("Should have contained %s", last), graph.contains(last));
	}
}
