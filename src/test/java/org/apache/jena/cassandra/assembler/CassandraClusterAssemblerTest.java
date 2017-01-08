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
package org.apache.jena.cassandra.assembler;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Set;

import org.apache.jena.cassandra.CassandraSetup;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

public class CassandraClusterAssemblerTest {
	
	private CassandraClusterAssembler assembler;
	private URL url;
	private Model model;
	
	@Before
	public void before() {
		assembler = new CassandraClusterAssembler();
		url = Thread.currentThread().getContextClassLoader().getResource( "assembler/cluster.ttl");
		model = ModelFactory.createDefaultModel();
		model.read( url.toString() );
	}
	
	@Test
	public void testRead() throws UnknownHostException
	{
		Object result = assembler.open( model.createResource( "http://example.com/cluster"));
		assertTrue( result instanceof Cluster);
		Cluster cluster = null;
		try {
		cluster = (Cluster) result;
		assertEquals( "testCluster", cluster.getClusterName());
			
		Configuration cfg = cluster.getConfiguration();
		assertNotNull( cfg.getMetricsOptions() );
		} finally {
			if (cluster != null) {cluster.close();}}
	}
	

}
