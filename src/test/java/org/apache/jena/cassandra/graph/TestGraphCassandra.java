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

import java.io.IOException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.jena.cassandra.CassandraSetup;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.testing_framework.AbstractGraphProducer;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.xenei.junit.contract.ContractImpl;
import org.xenei.junit.contract.ContractSuite;
import org.xenei.junit.contract.IProducer;
import org.xenei.junit.contract.Contract.Inject;

@RunWith(ContractSuite.class)
@ContractImpl(GraphCassandra.class)
public class TestGraphCassandra {

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
		connection = new CassandraConnection( cassandra.getCluster());
		connection.createKeyspace(String.format(
						"CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
						KEYSPACE));
		connection.deleteTables(KEYSPACE);
		connection.createTables(KEYSPACE);
	}

	@Inject
	public IProducer<GraphCassandra> getGraphProducer() {
		return graphProducer;
	}

	protected IProducer<GraphCassandra> graphProducer = new AbstractGraphProducer<GraphCassandra>() {

		@Override
		protected void afterClose(Graph g) {
			((GraphCassandra) g).performDelete(Triple.ANY);
		}

		int graphCount = 0;

		@Override
		protected GraphCassandra createNewGraph() {
			if (connection == null) {
				try {
					setupTestGraphCassandra();
				} catch (ConfigurationException | TTransportException | IOException | InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
			return new GraphCassandra(NodeFactory.createURI("http://example.com/graph" + graphCount++), KEYSPACE,
					connection);
		}

		@Override
		public Graph[] getDependsOn(Graph g) {
			return null;
		}

		@Override
		public Graph[] getNotDependsOn(Graph g) {
			return new Graph[] { createNewGraph() };
		}

	};

}
