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

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.jena.ext.com.google.common.io.Files;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestGraphCassandra {

	private GraphCassandra g;
	private CassandraConnection connection;
	private static final String KEYSPACE = "test";
	private static File tempDir;

	/**
	 * Set embedded cassandra up and spawn it in a new thread.
	 *
	 * @throws TTransportException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@BeforeClass
	public static void before() throws Exception, InterruptedException {
		tempDir = Files.createTempDir();
		// Tell cassandra where the configuration files are.
		// Use the test configuration file.
		// System.setProperty("storage-config", "../../test/conf");

		URL url = TestGraphCassandra.class.getClassLoader().getResource("cassandraTest.yaml");

		System.setProperty("cassandra.config", url.toString());
		System.setProperty("cassandra.storagedir", tempDir.toString());
		CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
		cleaner.prepare();
		cassandra = new EmbeddedCassandraService();
		cassandra.init();
		Thread t = new Thread(cassandra);
		t.setDaemon(true);
		t.start();

	}

	@AfterClass
	public static void after() {
		FileUtils.deleteRecursive(tempDir);
	}

	@Before
	public void setup() throws ConfigurationException, TTransportException, IOException, InterruptedException {

		connection = new CassandraConnection("localhost");
		connection.getSession().execute(String.format("CREATE KEYSPACE %s", KEYSPACE));

		g = new GraphCassandra(NodeFactory.createURI("http://example.com/graph"), KEYSPACE, connection);
		connection.deleteTables(KEYSPACE);
		connection.createTables(KEYSPACE);
	}

	@Test
	public void x() {
		Node s = NodeFactory.createBlankNode();
		Node o = NodeFactory.createURI("http://example.com/node");
		Triple t = new Triple(s, RDF.type.asNode(), o);
		g.add(t);
		t = new Triple(s, RDFS.comment.asNode(), NodeFactory.createLiteral("This is the comment"));
		g.add(t);
		ExtendedIterator<Triple> iter = g.find(s, Node.ANY, Node.ANY);
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}

		iter = g.find(Node.ANY, RDF.type.asNode(), Node.ANY);
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
	}

	private static EmbeddedCassandraService cassandra;

	// @Test
	// public void testInProcessCassandraServer()
	//// throws UnsupportedEncodingException, InvalidRequestException,
	//// UnavailableException, TimedOutException, TException,
	//// NotFoundException {
	// {
	// Cassandra.Client client = getClient();
	//
	// String key_user_id = "1";
	//
	// long timestamp = System.currentTimeMillis();
	// ColumnPath cp = new ColumnPath("Standard1");
	// cp.setColumn("name".getBytes("utf-8"));
	//
	// // insert
	// client.insert("Keyspace1", key_user_id, cp, "Ran".getBytes("UTF-8"),
	// timestamp, ConsistencyLevel.ONE);
	//
	// // read
	// ColumnOrSuperColumn got = client.get("Keyspace1", key_user_id, cp,
	// ConsistencyLevel.ONE);
	//
	// // assert
	// assertNotNull("Got a null ColumnOrSuperColumn", got);
	// assertEquals("Ran", new String(got.getColumn().getValue(), "utf-8"));
	// }
	//
	// /**
	// * Gets a connection to the localhost client
	// *
	// * @return
	// * @throws TTransportException
	// */
	// private Cassandra.Client getClient() throws TTransportException {
	// TTransport tr = new TSocket("localhost", 9170);
	// TProtocol proto = new TBinaryProtocol(tr);
	// Cassandra.Client client = new Cassandra.Client(proto);
	// tr.open();
	// return client;
	// }

}
