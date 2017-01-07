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
import org.apache.jena.cassandra.graph.CassandraConnection;
import org.apache.jena.cassandra.graph.CassandraSetup;
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

public class CassandraModelAssemblerTest {

private static CassandraSetup cassandra;
	
private CassandraModelAssembler assembler;
private URL url;
private Model model;
private File tmpFile;

	/**
	 * Set embedded cassandra up and spawn it in a new thread.
	 *
	 * @throws TTransportException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@BeforeClass
	public static void beforeClass() throws Exception, InterruptedException {
		cassandra = new CassandraSetup();
	}

	@AfterClass
	public static void afterClass() {
		cassandra.shutdown();
	}
	
	@Before
	public void before() throws IOException {
		assembler = new CassandraModelAssembler();
		url = Thread.currentThread().getContextClassLoader().getResource( "assembler/model.ttl");
		model = ModelFactory.createDefaultModel();
		model.read( url.toString() );
		int port = cassandra.getSslStoragePort();
		model.write(System.out, "TURTLE");
		Resource r = model.createResource( "http://example.com/cluster");
		Statement stmt = r.getProperty( VocabCassandra.port);
		stmt.changeLiteralObject( port );
		tmpFile = File.createTempFile("model", ".ttl");
		model.write( new FileOutputStream( tmpFile ), "TURTLE");
//		
//		connection = new CassandraConnection("localhost", cassandra.getSslStoragePort());
//		connection.getSession()
//				.execute(String.format(
//						"CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
//						KEYSPACE));
//		connection.deleteTables(KEYSPACE);
//		connection.createTables(KEYSPACE);
	}
	
	@After
	public void after() {
		tmpFile.delete();
	}
	
	@Test
	public void testRead() throws UnknownHostException
	{
		Cluster cluster =  Cluster.builder().addContactPoint("localhost").withPort(cassandra.getSslStoragePort()).build();
        Symbol symbol = Symbol.create(String.format( "%s/%s", VocabCassandra.Cluster.getURI(),
        		"Test Cluster")) ;
        ARQ.getContext().set(symbol, cluster);
        
		Object result = assembler.open( model.createResource( "http://example.com/model"));
		assertTrue( result instanceof Model);
		
	}
	
	@Test
	public void testReadFromScratch() throws UnknownHostException
	{
		VocabCassandra.init();
		Model model = AssemblerUtils.readAssemblerFile( tmpFile.toString() ); 
		
		Object result = Assembler.general.open(model.createResource( "http://example.com/model"));
		        
		assertTrue( result instanceof Model);
		
	}

}
