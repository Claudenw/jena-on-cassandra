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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.UnknownHostException;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.cassandra.CassandraSetup;
import org.apache.jena.cassandra.graph.CassandraConnection;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.sparql.util.Symbol;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;

public class CassandraDatasetAssemblerTest {
	
private CassandraDatasetAssembler assembler;
private URL url;
private Model model;


	
	@Before
	public void before() throws IOException {
		assembler = new CassandraDatasetAssembler();
		url = Thread.currentThread().getContextClassLoader().getResource( "assembler/dataset.ttl");
		model = ModelFactory.createDefaultModel();
		model.read( url.toString() );
	}
	
	@Test
	public void testRead() throws UnknownHostException
	{
		Cluster cluster =  Cluster.builder().addContactPoint("localhost").build();
        Symbol symbol = Symbol.create(String.format( "%s/%s", VocabCassandra.Cluster.getURI(),
        		"Test Cluster")) ;
        ARQ.getContext().set(symbol, cluster);
        
		Object result = assembler.open( model.createResource( "http://example.com/dataset"));
		assertTrue( result instanceof Dataset);
		
	}
	
	@Test
	public void testReadFromFile() throws UnknownHostException
	{
		VocabCassandra.init();
		Model model = AssemblerUtils.readAssemblerFile( url.toString() ); 
		
		Object result = Assembler.general.open(model.createResource( "http://example.com/dataset"));
		        
		assertTrue( result instanceof Dataset);
		
	}

}
