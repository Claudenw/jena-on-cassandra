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
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.LiteralLabelFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import static org.mockito.Mockito.*;

/**
 * Test the query info object.
 *
 */
public class QueryInfoTest {

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
	private static String node42LitValue = "42";

	private static String nodeLitValue = "'String Literal'";
	private static Node nodeLit = NodeFactory.createLiteral("String Literal");
	private static String nodeLitDType = "'http://www.w3.org/2001/XMLSchema#string'";
	private static String nodeLitHexValue = "0x0c00030b00010000000e537472696e67204c69746572616c0000";

	private static Node nodeLitLang = NodeFactory.createLiteral("String Literal", "en-US");
	private static String nodeLitLangDType = "'http://www.w3.org/1999/02/22-rdf-syntax-ns#langString'";
	private static String nodeLitLangHexValue = "0x0c00030b00010000000e537472696e67204c69746572616c0b000200000005656e2d55530000";

	private QueryPattern pattern;

	@Before
	public void setup() {
		Cluster cluster = mock(Cluster.class);
		Session session = mock(Session.class);
		when(cluster.connect()).thenReturn(session);
		CassandraConnection connection = new CassandraConnection(cluster);
		pattern = new QueryPattern(connection, new Quad(Node.ANY, Triple.ANY));
	}

	private void assertColumnNotFound(ColumnName c, String s) {
		assertFalse(c + " found in " + s, s.contains(c.toString()));
	}

	private void assertColumnDataFound(ColumnName c, String data, String s) {
		assertTrue(c + " data not found in " + s, s.contains(String.format("%s=%s", c, data)));
	}

	private void assertColumnScanFound(ColumnName c, String data, String s) {
		if (data == null) {
			assertTrue(c + " unlimited scan not found in " + s,
					s.contains(String.format("token(%s) >= %s", c, Long.MIN_VALUE)));
		} else {
			assertTrue(c + " value scan not found in " + s,
					s.contains(String.format("token(%s) = token(%s)", c, data)));
		}
	}

	@Test
	public void whereGSPOTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, object);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.SPOG, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnDataFound(ColumnName.O, objectHexValue, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void whereGSPOExtraTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, object);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraWhere = "something=Something";
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.SPOG, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnDataFound(ColumnName.O, objectHexValue, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
		assertTrue("extra missing", s.contains(" AND something=Something"));

	}

	@Test
	public void whereGSPNumTest() throws TException {

		Quad q = new Quad(graph, subject, predicate, node42);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnDataFound(ColumnName.I, node42LitValue, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void whereGSPLitTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, nodeLit);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnDataFound(ColumnName.D, nodeLitDType, s);
		assertColumnDataFound(ColumnName.V, nodeLitValue, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void whereGS_NumTest() throws TException {
		Quad q = new Quad(graph, subject, Node.ANY, node42);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnDataFound(ColumnName.I, node42LitValue, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void whereG_PNumTest() throws TException {
		Quad q = new Quad(graph, Node.ANY, predicate, node42);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.PGOS, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnDataFound(ColumnName.I, node42LitValue, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("filter needed", clause.needFilter);
	}

	@Test
	public void where_SPNumTest() throws TException {
		Quad q = new Quad(Node.ANY, subject, predicate, node42);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.SPOG, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.G, s);
		assertColumnDataFound(ColumnName.I, node42LitValue, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	// END OF EDIT
	@Test
	public void whereG__NumTest() throws TException {
		Quad q = new Quad(graph, Node.ANY, Node.ANY, node42);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		String s = clause.text.toString();

		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnDataFound(ColumnName.I, node42LitValue, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);

	}

	@Test
	public void where_S_NumTest() throws TException {
		Quad q = new Quad(Node.ANY, subject, Node.ANY, node42);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.SPOG, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnNotFound(ColumnName.G, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnDataFound(ColumnName.I, node42LitValue, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void where__PNumTest() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, predicate, node42);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.PGOS, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.G, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnDataFound(ColumnName.I, node42LitValue, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void where___NumTest() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, Node.ANY, node42);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L, ColumnName.D, ColumnName.V);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		String s = clause.text.toString();
		assertColumnScanFound(ColumnName.G, null, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnDataFound(ColumnName.I, node42LitValue, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void whereGSP_Test() throws TException {
		Quad q = new Quad(graph, subject, predicate, Node.ANY);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		String s = clause.text.toString();

		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);

	}

	@Test
	public void whereGS_OTest() throws TException {
		Quad q = new Quad(graph, subject, Node.ANY, object);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.OSGP, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.O, objectHexValue, s);
		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void whereG_POTest() throws TException {
		Quad q = new Quad(graph, Node.ANY, predicate, object);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.PGOS, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnDataFound(ColumnName.O, objectHexValue, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);

	}

	@Test
	public void where_SPOTest() throws TException {
		Quad q = new Quad(Node.ANY, subject, predicate, object);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.SPOG, qi.tableName);
		String s = clause.text.toString();
		assertColumnNotFound(ColumnName.G, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnDataFound(ColumnName.O, objectHexValue, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void whereGS__Test() throws TException {
		Quad q = new Quad(graph, subject, Node.ANY, Node.ANY);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		String s = clause.text.toString();

		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);

	}

	@Test
	public void whereG_P_Test() throws TException {
		Quad q = new Quad(graph, Node.ANY, predicate, Node.ANY);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.PGOS, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void where_SP_Test() throws TException {
		Quad q = new Quad(Node.ANY, subject, predicate, Node.ANY);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.SPOG, qi.tableName);
		String s = clause.text.toString();
		assertColumnNotFound(ColumnName.G, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void whereG__OTest() throws TException {
		Quad q = new Quad(graph, Node.ANY, Node.ANY, object);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.OSGP, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.O, objectHexValue, s);
		assertColumnNotFound(ColumnName.G, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertTrue("Filter needed", clause.needFilter);
	}

	@Test
	public void where_S_OTest() throws TException {
		Quad q = new Quad(Node.ANY, subject, Node.ANY, object);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.OSGP, qi.tableName);
		String s = clause.text.toString();
		assertColumnNotFound(ColumnName.G, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnDataFound(ColumnName.O, objectHexValue, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void where__POTest() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, predicate, object);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.PGOS, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.G, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertTrue("Filter needed", clause.needFilter);
	}

	@Test
	public void where___OTest() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, Node.ANY, object);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.OSGP, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.O, objectHexValue, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnNotFound(ColumnName.G, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void where__P_Test() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, predicate, Node.ANY);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.PGOS, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.G, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void where_S__Test() throws TException {
		Quad q = new Quad(Node.ANY, subject, Node.ANY, Node.ANY);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.SPOG, qi.tableName);
		String s = clause.text.toString();
		assertColumnNotFound(ColumnName.G, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void whereG___Test() throws TException {
		Quad q = new Quad(graph, Node.ANY, Node.ANY, Node.ANY);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		String s = clause.text.toString();

		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);

	}

	@Test
	public void where____Test() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		String s = clause.text.toString();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		assertColumnScanFound(ColumnName.G, null, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnNotFound(ColumnName.D, s);
		assertColumnNotFound(ColumnName.V, s);
		assertColumnNotFound(ColumnName.L, s);

	}

	@Test
	public void whereGS_LitTest() throws TException {
		Quad q = new Quad(graph, subject, Node.ANY, nodeLit);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		String s = clause.text.toString();

		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnDataFound(ColumnName.D, nodeLitDType, s);
		assertColumnDataFound(ColumnName.V, nodeLitValue, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);

	}

	@Test
	public void whereG_PLitTest() throws TException {
		Quad q = new Quad(graph, Node.ANY, predicate, nodeLit);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.PGOS, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnDataFound(ColumnName.D, nodeLitDType, s);
		assertColumnDataFound(ColumnName.V, nodeLitValue, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter needed", clause.needFilter);
	}

	@Test
	public void where_SPLitTest() throws TException {
		Quad q = new Quad(Node.ANY, subject, predicate, nodeLit);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.SPOG, qi.tableName);
		String s = clause.text.toString();
		assertColumnNotFound(ColumnName.G, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnDataFound(ColumnName.D, nodeLitDType, s);
		assertColumnDataFound(ColumnName.V, nodeLitValue, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void whereG__LitTest() throws TException {
		Quad q = new Quad(graph, Node.ANY, Node.ANY, nodeLit);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		String s = clause.text.toString();

		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnDataFound(ColumnName.D, nodeLitDType, s);
		assertColumnDataFound(ColumnName.V, nodeLitValue, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);

	}

	@Test
	public void where_S_LitTest() throws TException {
		Quad q = new Quad(Node.ANY, subject, Node.ANY, nodeLit);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.SPOG, qi.tableName);
		String s = clause.text.toString();

		assertColumnNotFound(ColumnName.G, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnDataFound(ColumnName.D, nodeLitDType, s);
		assertColumnDataFound(ColumnName.V, nodeLitValue, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);

	}

	@Test
	public void where__PLitTest() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, predicate, nodeLit);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.PGOS, qi.tableName);
		String s = clause.text.toString();
		assertColumnNotFound(ColumnName.G, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnDataFound(ColumnName.D, nodeLitDType, s);
		assertColumnDataFound(ColumnName.V, nodeLitValue, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void where___LitTest() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, Node.ANY, nodeLit);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		String s = clause.text.toString();
		assertColumnScanFound(ColumnName.G, null, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnDataFound(ColumnName.D, nodeLitDType, s);
		assertColumnDataFound(ColumnName.V, nodeLitValue, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void whereGS_LitLangTest() throws TException {
		Quad q = new Quad(graph, subject, Node.ANY, nodeLitLang);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		String s = clause.text.toString();

		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnDataFound(ColumnName.D, nodeLitLangDType, s);
		assertColumnDataFound(ColumnName.V, nodeLitValue, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);

	}

	@Test
	public void whereG_PLitLangTest() throws TException {
		Quad q = new Quad(graph, Node.ANY, predicate, nodeLitLang);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.PGOS, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnDataFound(ColumnName.D, nodeLitLangDType, s);
		assertColumnDataFound(ColumnName.V, nodeLitValue, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);

	}

	@Test
	public void where_SPLitLangTest() throws TException {
		Quad q = new Quad(Node.ANY, subject, predicate, nodeLitLang);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.SPOG, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.G, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnDataFound(ColumnName.D, nodeLitLangDType, s);
		assertColumnDataFound(ColumnName.V, nodeLitValue, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void whereG__LitLangTest() throws TException {
		Quad q = new Quad(graph, Node.ANY, Node.ANY, nodeLitLang);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.G, graphHexValue, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnDataFound(ColumnName.D, nodeLitLangDType, s);
		assertColumnDataFound(ColumnName.V, nodeLitValue, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);

	}

	@Test
	public void where_S_LitLangTest() throws TException {
		Quad q = new Quad(Node.ANY, subject, Node.ANY, nodeLitLang);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.SPOG, qi.tableName);
		String s = clause.text.toString();
		assertColumnNotFound(ColumnName.G, s);
		assertColumnDataFound(ColumnName.S, subjectHexValue, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnDataFound(ColumnName.D, nodeLitLangDType, s);
		assertColumnDataFound(ColumnName.V, nodeLitValue, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void where__PLitLangTest() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, predicate, nodeLitLang);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.PGOS, qi.tableName);
		String s = clause.text.toString();
		assertColumnDataFound(ColumnName.P, predicateHexValue, s);
		assertColumnNotFound(ColumnName.G, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnDataFound(ColumnName.D, nodeLitLangDType, s);
		assertColumnDataFound(ColumnName.V, nodeLitValue, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void where___LitLangTest() throws TException {
		Quad q = new Quad(Node.ANY, Node.ANY, Node.ANY, nodeLitLang);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = Arrays.asList(ColumnName.L);
		qi.tableQuad = new Quad(q.getGraph(), q.getSubject(), q.getPredicate(), Node.ANY);
		qi.values.remove(ColumnName.O);
		qi.tableName = CassandraConnection.getTable(CassandraConnection.getId(qi.tableQuad));
		QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		String s = clause.text.toString();
		assertEquals(CassandraConnection.GSPO, qi.tableName);
		assertColumnScanFound(ColumnName.G, null, s);
		assertColumnNotFound(ColumnName.S, s);
		assertColumnNotFound(ColumnName.P, s);
		assertColumnNotFound(ColumnName.O, s);
		assertColumnNotFound(ColumnName.I, s);
		assertColumnDataFound(ColumnName.D, nodeLitLangDType, s);
		assertColumnDataFound(ColumnName.V, nodeLitValue, s);
		assertColumnNotFound(ColumnName.L, s);
		assertFalse("Filter not needed", clause.needFilter);
	}

	@Test
	public void deleteObjectTest() throws TException {
		// Quad q = new Quad(Node.ANY, Node.ANY, Node.ANY, nodeLitLang);
		Quad q = new Quad(graph, subject, predicate, object);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = qi.getNonKeyColumns();
		for (TableName tableName : CassandraConnection.TABLES) {
			qi.tableName = tableName;
			QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
			assertEquals(tableName, qi.tableName);
			String s = clause.text.toString();
			assertColumnDataFound(ColumnName.G, graphHexValue, s);
			assertColumnDataFound(ColumnName.S, subjectHexValue, s);
			assertColumnDataFound(ColumnName.P, predicateHexValue, s);
			assertColumnDataFound(ColumnName.O, objectHexValue, s);
			assertColumnNotFound(ColumnName.I, s);
			assertColumnNotFound(ColumnName.D, s);
			assertColumnNotFound(ColumnName.V, s);
			assertColumnNotFound(ColumnName.L, s);
			assertFalse("Filter not needed", clause.needFilter);
		}
	}

	@Test
	public void deleteNumTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, node42);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = qi.getNonKeyColumns();
		for (TableName tableName : CassandraConnection.TABLES) {
			qi.tableName = tableName;
			QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
			assertEquals(tableName, qi.tableName);
			String s = clause.text.toString();
			assertColumnDataFound(ColumnName.G, graphHexValue, s);
			assertColumnDataFound(ColumnName.S, subjectHexValue, s);
			assertColumnDataFound(ColumnName.P, predicateHexValue, s);
			assertColumnDataFound(ColumnName.O, node42HexValue, s);
			assertColumnNotFound(ColumnName.I, s);
			assertColumnNotFound(ColumnName.D, s);
			assertColumnNotFound(ColumnName.V, s);
			assertColumnNotFound(ColumnName.L, s);
			assertFalse("Filter not needed", clause.needFilter);
		}

	}

	@Test
	public void deleteLitTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, nodeLit);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = qi.getNonKeyColumns();
		for (TableName tableName : CassandraConnection.TABLES) {
			qi.tableName = tableName;
			QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
			assertEquals(tableName, qi.tableName);
			String s = clause.text.toString();
			assertColumnDataFound(ColumnName.G, graphHexValue, s);
			assertColumnDataFound(ColumnName.S, subjectHexValue, s);
			assertColumnDataFound(ColumnName.P, predicateHexValue, s);
			assertColumnDataFound(ColumnName.O, nodeLitHexValue, s);
			assertColumnNotFound(ColumnName.I, s);
			assertColumnNotFound(ColumnName.D, s);
			assertColumnNotFound(ColumnName.V, s);
			assertColumnNotFound(ColumnName.L, s);
			assertFalse("Filter not needed", clause.needFilter);
		}

	}

	@Test
	public void deleteLitLangTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, nodeLitLang);
		QueryPattern.QueryInfo qi = pattern.getQueryInfo(q);
		qi.extraValueFilter = qi.getNonKeyColumns();
		for (TableName tableName : CassandraConnection.TABLES) {
			qi.tableName = tableName;
			QueryPattern.QueryInfo.WhereClause clause = qi.getWhereClause();
			assertEquals(tableName, qi.tableName);
			String s = clause.text.toString();
			assertColumnDataFound(ColumnName.G, graphHexValue, s);
			assertColumnDataFound(ColumnName.S, subjectHexValue, s);
			assertColumnDataFound(ColumnName.P, predicateHexValue, s);
			assertColumnDataFound(ColumnName.O, nodeLitLangHexValue, s);
			assertColumnNotFound(ColumnName.I, s);
			assertColumnNotFound(ColumnName.D, s);
			assertColumnNotFound(ColumnName.V, s);
			assertColumnNotFound(ColumnName.L, s);
			assertFalse("Filter not needed", clause.needFilter);
		}

	}

}
