package org.apache.jena.cassandra.graph;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Quad;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class DeleteGeneratorTest {

	private DeleteGenerator generator;
	private CassandraConnection connection;
	private MyResultSet myResultSet;
	
	static String helloHex = "0x48656c6c6f ";
	static String worldHex = "0x576f726c64 ";
	
	@Before
	public void setup() {
		Quad q = new Quad( NodeFactory.createURI("http://exmaple.com/graph"), 
				NodeFactory.createURI("http://exmaple.com/subject"), 
				NodeFactory.createURI("http://exmaple.com/predicate"), 
				NodeFactory.createURI("http://exmaple.com/object"));
		QueryPattern qp = new QueryPattern(q);
		connection = mock( CassandraConnection.class );
		Session mockSession = mock( Session.class );
		Row mockRow = mock( Row.class );
		when( mockRow.getBytes(0)).thenReturn( ByteBuffer.wrap( "Hello".getBytes()));
		
		Row mockRow2 = mock( Row.class );
		when( mockRow2.getBytes(0)).thenReturn( ByteBuffer.wrap( "World".getBytes()));
		
		
		myResultSet = new MyResultSet( mockRow, mockRow2 );
		
		when( connection.getSession()).thenReturn( mockSession );
		when( mockSession.execute( any(String.class))).thenReturn( myResultSet );
		
	}
		
	@Test
	public void fullyQualifiedTest()
	{
		Quad q = new Quad( NodeFactory.createURI("http://exmaple.com/graph"), 
				NodeFactory.createURI("http://exmaple.com/subject"), 
				NodeFactory.createURI("http://exmaple.com/predicate"),
				NodeFactory.createURI("http://exmaple.com/object"));
		QueryPattern qp = new QueryPattern(q);
		generator = new DeleteGenerator(connection, "test", qp );
		
		assertTrue( generator.hasNext() );
		String stmt = generator.next();
		assertTrue( stmt.contains(" test.GSPO "));

		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.OSGP "));

		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.POGS "));
	
		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.SPOG "));
		
		
		assertFalse( generator.hasNext() );
	}
	
	@Test
	public void nullSubjectTest()
	{
		String hello = " subject="+helloHex;
		String world = " subject="+worldHex;
		
		Quad q = new Quad( NodeFactory.createURI("http://exmaple.com/graph"), 
				Node.ANY, 
				NodeFactory.createURI("http://exmaple.com/"), 
				NodeFactory.createURI("http://exmaple.com/object"));
		QueryPattern qp = new QueryPattern(q);
		generator = new DeleteGenerator(connection, "test", qp );
		
		assertTrue( generator.hasNext() );
		String stmt = generator.next();
		assertTrue( stmt.contains(" test.GSPO "));
		assertTrue( stmt.contains( hello ));

		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.GSPO "));
		assertTrue( stmt.contains( world ));

		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.OSGP "));
		assertTrue( stmt.contains( hello ));
		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.OSGP "));
		assertTrue( stmt.contains( world ));
		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.POGS "));
		assertFalse( stmt.contains( hello ));
		assertFalse( stmt.contains( world ));
		assertFalse( stmt.contains( "subject=" ));
		
		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.SPOG "));
		assertTrue( stmt.contains( hello ));
		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.SPOG "));
		assertTrue( stmt.contains( world ));
		
		assertFalse( generator.hasNext() );
	}
	
	@Test
	public void nullGraphTest()
	{
		String hello = " graph="+helloHex;
		String world = " graph="+worldHex;
		
		Quad q = new Quad( 
				Node.ANY, 
				NodeFactory.createURI("http://exmaple.com/subject"), 
				NodeFactory.createURI("http://exmaple.com/"), 
				NodeFactory.createURI("http://exmaple.com/object"));
		QueryPattern qp = new QueryPattern(q);
		generator = new DeleteGenerator(connection, "test", qp );
		
		assertTrue( generator.hasNext() );
		String stmt = generator.next();
		assertTrue( stmt.contains(" test.GSPO "));
		assertTrue( stmt.contains( hello ));

		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.GSPO "));
		assertTrue( stmt.contains( world ));

		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.OSGP "));
		assertTrue( stmt.contains( hello ));
		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.OSGP "));
		assertTrue( stmt.contains( world ));
		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.POGS "));
		assertTrue( stmt.contains( hello ));

		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.POGS "));
		assertTrue( stmt.contains( world ));
		
		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.SPOG "));
		assertFalse( stmt.contains( hello ));
		assertFalse( stmt.contains( world ));
		assertFalse( stmt.contains( "graph=" ));
		
	
		assertFalse( generator.hasNext() );
	}
	
	@Test
	public void dualNullTest()
	{
		String gHello = " graph="+helloHex;
		String gWorld = " graph="+worldHex;
		String sHello = " subject="+helloHex;
		String sWorld = " subject="+worldHex;
		
		Quad q = new Quad( 
				Node.ANY, 
				Node.ANY,
				NodeFactory.createURI("http://exmaple.com/"), 
				NodeFactory.createURI("http://exmaple.com/object"));
		QueryPattern qp = new QueryPattern(q);
		generator = new DeleteGenerator(connection, "test", qp );
		
		assertTrue( generator.hasNext() );
		String stmt = generator.next();
		assertTrue( stmt.contains(" test.GSPO "));
		assertTrue( stmt.contains( gHello ));
		assertTrue( stmt.contains( sHello ));

		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.GSPO "));
		assertTrue( stmt.contains( gHello ));
		assertTrue( stmt.contains( sWorld));

		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.GSPO "));
		assertTrue( stmt.contains( gWorld ));
		assertTrue( stmt.contains( sHello ));

		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.GSPO "));
		assertTrue( stmt.contains( gWorld ));
		assertTrue( stmt.contains( sWorld));

		/* OSGP */
		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.OSGP "));
		assertTrue( stmt.contains( sHello ));
		assertTrue( stmt.contains( gHello ));

		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.OSGP "));
		assertTrue( stmt.contains( sHello ));
		assertTrue( stmt.contains( gWorld));

		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.OSGP "));
		assertTrue( stmt.contains( sWorld ));
		assertTrue( stmt.contains( gHello ));

		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.OSGP "));
		assertTrue( stmt.contains( sWorld ));
		assertTrue( stmt.contains( gWorld));

		/* POGS */
		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.POGS "));
		assertFalse( stmt.contains( sHello ));
		assertFalse( stmt.contains( gHello ));
		assertFalse( stmt.contains( "graph="));
		assertFalse( stmt.contains( "statement="));
		
		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.SPOG "));
		assertTrue( stmt.contains( sHello ));
		assertFalse( stmt.contains( "graph=" ));
		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.SPOG "));
		assertTrue( stmt.contains( sWorld ));
		assertFalse( stmt.contains( "graph=" ));

		assertFalse( generator.hasNext() );
	}
	
	@Test
	public void fullyQualifiedEmptyGraphDeleteTest() {
		myResultSet.clear();
		
		Quad q = new Quad( NodeFactory.createURI("http://exmaple.com/graph"), 
				NodeFactory.createURI("http://exmaple.com/subject"), 
				NodeFactory.createURI("http://exmaple.com/predicate"),
				NodeFactory.createURI("http://exmaple.com/object"));
		
		QueryPattern qp = new QueryPattern(q);
		generator = new DeleteGenerator(connection, "test", qp );
		
		assertTrue( generator.hasNext() );
		String stmt = generator.next();
		assertTrue( stmt.contains(" test.GSPO "));

		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.OSGP "));

		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.POGS "));
	
		
		assertTrue( generator.hasNext() );
		stmt = generator.next();
		assertTrue( stmt.contains(" test.SPOG "));
		
		
		assertFalse( generator.hasNext() );
	}
	
	@Test
	public void nullSubjectEmptyGraphDeleteTest() {
		myResultSet.clear();
		
		Quad q = new Quad( NodeFactory.createURI("http://exmaple.com/graph"), 
				Node.ANY, 
				NodeFactory.createURI("http://exmaple.com/predicate"),
				NodeFactory.createURI("http://exmaple.com/object"));
		
		QueryPattern qp = new QueryPattern(q);
		generator = new DeleteGenerator(connection, "test", qp );
		
		assertFalse( generator.hasNext() );
	}

	private class MyResultSet implements ResultSet {
		List<Row> rows;

		MyResultSet(Row ...rows )
		{
			this.rows = Arrays.asList( rows );
		}
		
		public void clear() {
			rows = Collections.emptyList();
		}
		@Override
		public boolean isExhausted() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isFullyFetched() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public int getAvailableWithoutFetching() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public ListenableFuture<ResultSet> fetchMoreResults() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public List<Row> all() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Iterator<Row> iterator() {
			return rows.iterator();
		}

		@Override
		public ExecutionInfo getExecutionInfo() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public List<ExecutionInfo> getAllExecutionInfo() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Row one() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ColumnDefinitions getColumnDefinitions() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean wasApplied() {
			// TODO Auto-generated method stub
			return false;
		}
		
	}
}
