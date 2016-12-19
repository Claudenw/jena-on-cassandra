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

import java.util.Arrays;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.impl.LiteralLabelFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import static org.mockito.Mockito.*;

/**
 * Test the query pattern generator.
 * 
 * Where specific tests are in the QueryInfoTest.
 *
 */
public class QueryPatternTest {

	private static String graphHexValue = "0x0c00010b000100000018687474703a2f2f65786d61706c652e636f6d2f67726170680000";

	private static String subjectHexValue = "0x0c00010b00010000001a687474703a2f2f65786d61706c652e636f6d2f7375626a6563740000";

	private static String predicateHexValue = "0x0c00010b00010000001c687474703a2f2f65786d61706c652e636f6d2f7072656469636174650000";

	private static String objectHexValue = "0x0c00010b000100000019687474703a2f2f65786d61706c652e636f6d2f6f626a6563740000";

	private static Node graph = NodeFactory.createURI("http://exmaple.com/graph");
	private static Node subject = NodeFactory.createURI("http://exmaple.com/subject");
	private static Node predicate = NodeFactory.createURI("http://exmaple.com/predicate");
	private static Node object = NodeFactory.createURI("http://exmaple.com/object");

	private static Node node42 = NodeFactory.createLiteral(LiteralLabelFactory.createTypedLiteral(42));
	private static String node42HexValue = "0x0c00030b00010000000234320b000300000024687474703a2f2f7777772e77332e6f72672f323030312f584d4c536368656d6123696e740000";
	private static String node42DType = "'http://www.w3.org/2001/XMLSchema#int'";
	private static String node42LitValue = "'42'";

	private static String nodeLitValue = "'String Literal'";
	private static Node nodeLit = NodeFactory.createLiteral("String Literal");
	private static String nodeLitDType = "'http://www.w3.org/2001/XMLSchema#string'";
	private static String nodeLitHexValue = "0x0c00030b00010000000e537472696e67204c69746572616c0000";

	private static Node nodeLitLang = NodeFactory.createLiteral("String Literal", "en-US");
	private static String nodeLitLangDType = "'http://www.w3.org/1999/02/22-rdf-syntax-ns#langString'";
	private static String nodeLitLangHexValue = "0x0c00030b00010000000e537472696e67204c69746572616c0b000200000005656e2d55530000";

	private final static String SELECT_COLUMNS;

	private CassandraConnection connection;

	static {
		ColumnName[] cols = new ColumnName[ColumnName.values().length];
		for (ColumnName c : ColumnName.values()) {
			if (c.getQueryPos() != -1) {
				cols[c.getQueryPos()] = c;
			}

		}
		StringBuilder sb = new StringBuilder();
		for (ColumnName c : cols) {
			if (c == null) {
				break;
			}
			if (sb.length() > 0) {
				sb.append(", ");
			}
			sb.append(c);

		}
		SELECT_COLUMNS = sb.toString();
	}

	@Before
	public void setup() {
		Cluster cluster = mock(Cluster.class);
		Session session = mock(Session.class);
		when(cluster.connect()).thenReturn(session);
		connection = new CassandraConnection(cluster);
	}

	@Test
	public void findGSPOTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, object);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		;
		String s = query.text.toString();
		assertTrue("table missing", s.contains("test.SPOG"));
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findGSPOSuffixTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, object);
		QueryPattern qp = new QueryPattern(connection, q);

		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.suffix = "limit 1";
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("table missing", s.contains("test.SPOG"));
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));

		assertTrue("suffix missing", s.contains(" limit 1"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findGSPOExtraTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, object);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraWhere = "something=Something";
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("table missing", s.contains("test.SPOG"));
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("extra missing", s.contains(" AND something=Something"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findGSPOExtraSuffixTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, object);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraWhere = "something=Something";
		qi.suffix = "limit 1";
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("table missing", s.contains("test.SPOG"));
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("extra missing", s.contains(" AND something=Something "));
		assertTrue("suffix missing", s.contains(" limit 1"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findGSPNumTest() throws TException {

		Quad q = new Quad(graph, subject, predicate, node42);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findGSPLitTest() throws TException {

		Quad q = new Quad(graph, subject, predicate, nodeLit);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findGS_NumTest() throws TException {
		Quad q = new Quad(graph, subject, Node.ANY, node42);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findG_PNumTest() throws TException {
		Quad q = new Quad(graph, Node.ANY, predicate, node42);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.PGOS"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find_SPNumTest() throws TException {
		Quad q = new Quad(Node.ANY, subject, predicate, node42);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.SPOG"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findG__NumTest() throws TException {
		Quad q = new Quad(graph, Node.ANY, Node.ANY, node42);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find_S_NumTest() throws TException {
		Quad q = new Quad(Node.ANY, subject, Node.ANY, node42);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.SPOG"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find__PNumTest() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, predicate, node42);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.PGOS"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find___NumTest() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, Node.ANY, node42);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findGSP_Test() throws TException {
		Quad q = new Quad(graph, subject, predicate, Node.ANY);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.GSPO"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findGS_OTest() throws TException {
		Quad q = new Quad(graph, subject, Node.ANY, object);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.OSGP"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findG_POTest() throws TException {
		Quad q = new Quad(graph, Node.ANY, predicate, object);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.PGOS"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find_SPOTest() throws TException {
		Quad q = new Quad(Node.ANY, subject, predicate, object);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.SPOG"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findGS__Test() throws TException {
		Quad q = new Quad(graph, subject, Node.ANY, Node.ANY);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.GSPO"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findG_P_Test() throws TException {
		Quad q = new Quad(graph, Node.ANY, predicate, Node.ANY);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.PGOS"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find_SP_Test() throws TException {
		Quad q = new Quad(Node.ANY, subject, predicate, Node.ANY);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.SPOG"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findG__OTest() throws TException {
		Quad q = new Quad(graph, Node.ANY, Node.ANY, object);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.OSGP"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertTrue(query.needsFilter);
	}

	@Test
	public void find_S_OTest() throws TException {
		Quad q = new Quad(Node.ANY, subject, Node.ANY, object);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.OSGP"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find__POTest() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, predicate, object);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.PGOS"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertTrue(query.needsFilter);
	}

	@Test
	public void find___OTest() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, Node.ANY, object);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.OSGP"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find__P_Test() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, predicate, Node.ANY);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();

		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.PGOS"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find_S__Test() throws TException {
		Quad q = new Quad(Node.ANY, subject, Node.ANY, Node.ANY);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.SPOG"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findG___Test() throws TException {
		Quad q = new Quad(graph, Node.ANY, Node.ANY, Node.ANY);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.GSPO"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find____Test() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.GSPO"));
		assertFalse(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findGS_LitTest() throws TException {
		Quad q = new Quad(graph, subject, Node.ANY, nodeLit);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findG_PLitTest() throws TException {
		Quad q = new Quad(graph, Node.ANY, predicate, nodeLit);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.PGOS"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find_SPLitTest() throws TException {
		Quad q = new Quad(Node.ANY, subject, predicate, nodeLit);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.SPOG"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findG__LitTest() throws TException {
		Quad q = new Quad(graph, Node.ANY, Node.ANY, nodeLit);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find_S_LitTest() throws TException {
		Quad q = new Quad(Node.ANY, subject, Node.ANY, nodeLit);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.SPOG"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find__PLitTest() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, predicate, nodeLit);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.PGOS"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find___LitTest() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, Node.ANY, nodeLit);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findGS_LitLangTest() throws TException {
		Quad q = new Quad(graph, subject, Node.ANY, nodeLitLang);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findG_PLitLangTest() throws TException {
		Quad q = new Quad(graph, Node.ANY, predicate, nodeLitLang);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.PGOS"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find_SPLitLangTest() throws TException {
		Quad q = new Quad(Node.ANY, subject, predicate, nodeLitLang);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.SPOG"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void findG__LitLangTest() throws TException {
		Quad q = new Quad(graph, Node.ANY, Node.ANY, nodeLitLang);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find_S_LitLangTest() throws TException {
		Quad q = new Quad(Node.ANY, subject, Node.ANY, nodeLitLang);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.SPOG"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find__PLitLangTest() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, predicate, nodeLitLang);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.PGOS"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	@Test
	public void find___LitLangTest() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, Node.ANY, nodeLitLang);
		QueryPattern qp = new QueryPattern(connection, q);
		QueryPattern.QueryInfo qi = qp.getQueryInfo();
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.Query query = qp.getFindQuery("test", qi);
		String s = query.text.toString();
		assertTrue("Standard query columns missing", s.contains("SELECT " + SELECT_COLUMNS + " FROM "));
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue(s.contains("ALLOW FILTERING"));
		assertFalse(query.needsFilter);
	}

	public void verifyInsertNumericValues(String line) {
		assertTrue("insert columns missing: " + line, line.contains("(subject, predicate, object, graph, "
				+ ColumnName.D + ", " + ColumnName.I + ", " + ColumnName.V + ")"));
		assertTrue("graphHexValue missing from " + line, line.contains(" " + graphHexValue + ", "));
		assertTrue("subjectHexValue missing from " + line, line.contains("(" + subjectHexValue + ", "));
		assertTrue("predicateHexValue missing from " + line, line.contains(" " + predicateHexValue + ", "));
		assertTrue("node42HexValue missing from " + line, line.contains(" " + node42HexValue + ", "));
		assertTrue("data type missing from " + line, line.contains(", " + node42DType + ", "));
		assertTrue("42 missing from " + line, line.contains(" 42,"));
		assertTrue("literal value missing from " + line, line.contains(", " + node42LitValue + ");"));
	}

	@Test
	public void insertNumTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, node42);
		QueryPattern qp = new QueryPattern(connection, q);
		String s = qp.getInsertStatement("test");
		String[] lines = s.split("\n");
		assertEquals(6, lines.length);
		assertEquals("BEGIN BATCH", lines[0]);

		assertTrue("table missing", lines[1].contains("test.SPOG"));
		verifyInsertNumericValues(lines[1]);

		assertTrue("table missing", lines[2].contains("test.PGOS"));
		verifyInsertNumericValues(lines[2]);

		assertTrue("table missing", lines[3].contains("test.OSGP"));
		verifyInsertNumericValues(lines[3]);

		assertTrue("table missing", lines[4].contains("test.GSPO"));
		verifyInsertNumericValues(lines[4]);

		assertEquals("APPLY BATCH;", lines[5]);

	}

	private void verifyInsertLitValues(String line) {
		assertTrue("insert columns missing: " + line,
				line.contains("(subject, predicate, object, graph, " + ColumnName.D + ", " + ColumnName.V + ")"));
		assertTrue("graphHexValue missing from " + line, line.contains(" " + graphHexValue + ", "));
		assertTrue("subjectHexValue missing from " + line, line.contains("(" + subjectHexValue + ", "));
		assertTrue("predicateHexValue missing from " + line, line.contains(" " + predicateHexValue + ", "));
		assertTrue("litHexValue missing from " + line, line.contains(" " + nodeLitHexValue + ", "));
		assertTrue("data type missing from " + line, line.contains(", " + nodeLitDType + ", "));
		assertTrue("literal value missing from " + line, line.contains(", " + nodeLitValue + ");"));
	}

	@Test
	public void insertLitTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, nodeLit);
		QueryPattern qp = new QueryPattern(connection, q);
		String s = qp.getInsertStatement("test");
		String[] lines = s.split("\n");
		assertEquals(6, lines.length);
		assertEquals("BEGIN BATCH", lines[0]);

		assertTrue("table missing", lines[1].contains("test.SPOG"));
		verifyInsertLitValues(lines[1]);

		assertTrue("table missing", lines[2].contains("test.PGOS"));
		verifyInsertLitValues(lines[2]);

		assertTrue("table missing", lines[3].contains("test.OSGP"));
		verifyInsertLitValues(lines[3]);

		assertTrue("table missing", lines[4].contains("test.GSPO"));
		verifyInsertLitValues(lines[4]);

		assertEquals("APPLY BATCH;", lines[5]);

	}

	private void verifyInsertLitLangValues(String line) {
		assertTrue("insert columns missing: " + line, line.contains("(subject, predicate, object, graph, "
				+ ColumnName.L + ", " + ColumnName.D + ", " + ColumnName.V + ")"));
		assertTrue("graphHexValue missing from " + line, line.contains(" " + graphHexValue + ", "));
		assertTrue("subjectHexValue missing from " + line, line.contains("(" + subjectHexValue + ", "));
		assertTrue("predicateHexValue missing from " + line, line.contains(" " + predicateHexValue + ", "));
		assertTrue("litHexValue missing from " + line, line.contains(" " + nodeLitLangHexValue + ", "));
		assertTrue("data type missing from " + line, line.contains(", " + nodeLitLangDType + ", "));
		assertTrue("language type missing from " + line, line.contains(", 'en-US', "));
		assertTrue("literal value missing from " + line, line.contains(", " + nodeLitValue + ");"));
	}

	@Test
	public void insertLitLangText() throws TException {
		Quad q = new Quad(graph, subject, predicate, nodeLitLang);
		QueryPattern qp = new QueryPattern(connection, q);
		String s = qp.getInsertStatement("test");
		String[] lines = s.split("\n");
		assertEquals(6, lines.length);
		assertEquals("BEGIN BATCH", lines[0]);

		assertTrue("table missing", lines[1].contains("test.SPOG"));
		verifyInsertLitLangValues(lines[1]);

		assertTrue("table missing", lines[2].contains("test.PGOS"));
		verifyInsertLitLangValues(lines[2]);

		assertTrue("table missing", lines[3].contains("test.OSGP"));
		verifyInsertLitLangValues(lines[3]);

		assertTrue("table missing", lines[4].contains("test.GSPO"));
		verifyInsertLitLangValues(lines[4]);

		assertEquals("APPLY BATCH;", lines[5]);

	}

	private void verifyInsertObjectValues(String line) {
		assertTrue("insert columns missing: " + line, line.contains("(subject, predicate, object, graph)"));
		assertTrue("graphHexValue missing from " + line, line.contains(" " + graphHexValue + ");"));
		assertTrue("subjectHexValue missing from " + line, line.contains("(" + subjectHexValue + ", "));
		assertTrue("predicateHexValue missing from " + line, line.contains(" " + predicateHexValue + ", "));
		assertTrue("objectHexValue missing from " + line, line.contains(" " + objectHexValue + ", "));
	}

	@Test
	public void insertObjectTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, object);
		QueryPattern qp = new QueryPattern(connection, q);
		String s = qp.getInsertStatement("test");
		String[] lines = s.split("\n");
		assertEquals(6, lines.length);
		assertEquals("BEGIN BATCH", lines[0]);

		assertTrue("table missing", lines[1].contains("test.SPOG"));
		verifyInsertObjectValues(lines[1]);

		assertTrue("table missing", lines[2].contains("test.PGOS"));
		verifyInsertObjectValues(lines[2]);

		assertTrue("table missing", lines[3].contains("test.OSGP"));
		verifyInsertObjectValues(lines[3]);

		assertTrue("table missing", lines[4].contains("test.GSPO"));
		verifyInsertObjectValues(lines[4]);

		assertEquals("APPLY BATCH;", lines[5]);

	}

	@Test
	public void deleteObjectTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, object);
		QueryPattern qp = new QueryPattern(connection, q);
		String s = qp.getDeleteStatement("test", q);
		String[] lines = s.split("\n");
		assertEquals(6, lines.length);
		assertEquals("BEGIN BATCH", lines[0]);

		assertTrue("table missing", lines[1].contains("test.SPOG"));

		assertTrue("table missing", lines[2].contains("test.PGOS"));

		assertTrue("table missing", lines[3].contains("test.OSGP"));

		assertTrue("table missing", lines[4].contains("test.GSPO"));

		assertEquals("APPLY BATCH;", lines[5]);
	}

	@Test
	public void deleteNumTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, node42);
		QueryPattern qp = new QueryPattern(connection, q);
		String s = qp.getDeleteStatement("test", q);
		String[] lines = s.split("\n");
		assertEquals(6, lines.length);
		assertEquals("BEGIN BATCH", lines[0]);

		assertTrue("table missing", lines[1].contains("test.SPOG"));

		assertTrue("table missing", lines[2].contains("test.PGOS"));

		assertTrue("table missing", lines[3].contains("test.OSGP"));

		assertTrue("table missing", lines[4].contains("test.GSPO"));

		assertEquals("APPLY BATCH;", lines[5]);
	}

	@Test
	public void deleteLitTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, nodeLit);
		QueryPattern qp = new QueryPattern(connection, q);
		String s = qp.getDeleteStatement("test", q);
		String[] lines = s.split("\n");
		assertEquals(6, lines.length);
		assertEquals("BEGIN BATCH", lines[0]);

		assertTrue("table missing", lines[1].contains("test.SPOG"));

		assertTrue("table missing", lines[2].contains("test.PGOS"));

		assertTrue("table missing", lines[3].contains("test.OSGP"));

		assertTrue("table missing", lines[4].contains("test.GSPO"));

		assertEquals("APPLY BATCH;", lines[5]);
	}

	@Test
	public void deleteLitLangTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, nodeLitLang);
		QueryPattern qp = new QueryPattern(connection, q);
		String s = qp.getDeleteStatement("test", q);
		String[] lines = s.split("\n");
		assertEquals(6, lines.length);
		assertEquals("BEGIN BATCH", lines[0]);

		assertTrue("table missing", lines[1].contains("test.SPOG"));

		assertTrue("table missing", lines[2].contains("test.PGOS"));

		assertTrue("table missing", lines[3].contains("test.OSGP"));

		assertTrue("table missing", lines[4].contains("test.GSPO"));

		assertEquals("APPLY BATCH;", lines[5]);
	}
}
