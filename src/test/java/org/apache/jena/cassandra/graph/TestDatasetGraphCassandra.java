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
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.cassandra.CassandraSetup;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.xenei.junit.contract.ContractImpl;
import org.xenei.junit.contract.ContractSuite;
import org.xenei.junit.contract.IProducer;

import org.xenei.junit.contract.Contract.Inject;

/**
 * Contract test suite for Cassandra DatasetGraph implementation.
 *
 */
@RunWith(ContractSuite.class)
@ContractImpl(DatasetGraphCassandra.class)
public class TestDatasetGraphCassandra {

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
	public static void afterClass() {
		cassandra.shutdown();
	}

	@After
	public void after() {
	    connection.close();
	}

	@Before
	public void setupTestGraphCassandra()
			throws TTransportException, IOException, InterruptedException {
		connection = new CassandraConnection(5,cassandra.getCluster(), cassandra.getJMXFactory());
		connection.createKeyspace(String.format(
				"CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
				KEYSPACE));
		// connection.deleteTables(KEYSPACE);
		connection.createTables(KEYSPACE);
		connection.truncateTables(KEYSPACE);
	}

	@Inject
	public IProducer<DatasetGraphCassandra> getGraphProducer() {
		return graphProducer;
	}

	protected IProducer<DatasetGraphCassandra> graphProducer = new IProducer<DatasetGraphCassandra>() {

		List<DatasetGraphCassandra> lst = new ArrayList<DatasetGraphCassandra>();

		@Override
		public DatasetGraphCassandra newInstance() {
			if (connection == null) {
				try {
					setupTestGraphCassandra();
				} catch (TTransportException | IOException | InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
			DatasetGraphCassandra dsg = new DatasetGraphCassandra(KEYSPACE, connection);
			lst.add(dsg);
			return dsg;
		}

		@Override
		public void cleanUp() {
			for (DatasetGraphCassandra dsg : lst) {
				dsg.close();
			}
			connection.truncateTables(KEYSPACE);
		}

	};

}
