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
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.impl.LiteralLabelFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.thrift.TException;
import org.junit.Test;

public class QueryPatternTest {

	private static String graphHexValue = "0x0c00010b000100000018687474703a2f2f65786d61706c652e636f6d2f67726170680000";
	private static String graphHex = " graph=" + graphHexValue + " ";

	private static String subjectHexValue = "0x0c00010b00010000001a687474703a2f2f65786d61706c652e636f6d2f7375626a6563740000";
	private static String subjectHex = " subject=" + subjectHexValue + " ";

	private static String predicateHexValue = "0x0c00010b00010000001c687474703a2f2f65786d61706c652e636f6d2f7072656469636174650000";
	private static String predicateHex = " predicate=" + predicateHexValue + " ";

	private static String objectHexValue = "0x0c00010b000100000019687474703a2f2f65786d61706c652e636f6d2f6f626a6563740000";
	private static String objectHex = " object=" + objectHexValue + " ";

	private static Node graph = NodeFactory.createURI("http://exmaple.com/graph");
	private static Node subject = NodeFactory.createURI("http://exmaple.com/subject");
	private static Node predicate = NodeFactory.createURI("http://exmaple.com/predicate");
	private static Node object = NodeFactory.createURI("http://exmaple.com/object");

	private static Node node42 = NodeFactory.createLiteral(LiteralLabelFactory.createTypedLiteral(42));
	private static String node42HexValue = "0x0c00010b000100000018687474703a2f2f65786d61706c652e636f6d2f67726170680000";

	
	@Test
	public void fullFindQueryTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, object);
		QueryPattern qp = new QueryPattern(q);

		// add extra space to match internal tests
		String s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.SPOG"));
		assertTrue("graph missing", s.contains(graphHex));
		assertTrue("subject missing", s.contains(subjectHex));
		assertTrue("predicate missing", s.contains(predicateHex));
		assertTrue("object missing", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));

	}

	@Test
	public void fullFindQuerySuffixTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, object);
		QueryPattern qp = new QueryPattern(q);

		// add extra space to match internal tests
		String s = qp.getFindQuery("test", null, "limit 1");
		assertTrue("table missing", s.contains("test.SPOG"));
		assertTrue("graph missing", s.contains(graphHex));
		assertTrue("subject missing", s.contains(subjectHex));
		assertTrue("predicate missing", s.contains(predicateHex));
		assertTrue("object missing", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));
		assertTrue("suffix missing", s.contains(" limit 1"));
	}

	@Test
	public void fullFindQueryExtraTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, object);
		QueryPattern qp = new QueryPattern(q);

		// add extra space to match internal tests
		String s = qp.getFindQuery("test", "something=Something", null);
		assertTrue("table missing", s.contains("test.SPOG"));
		assertTrue("graph missing", s.contains(graphHex));
		assertTrue("subject missing", s.contains(subjectHex));
		assertTrue("predicate missing", s.contains(predicateHex));
		assertTrue("object missing", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));
		assertTrue("extra missing", s.contains(" AND something=Something"));
	}

	@Test
	public void fullFindQueryExtraSuffixTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, object);
		QueryPattern qp = new QueryPattern(q);

		// add extra space to match internal tests
		String s = qp.getFindQuery("test", "something=Something", "limit 1");
		assertTrue("table missing", s.contains("test.SPOG"));
		assertTrue("graph missing", s.contains(graphHex));
		assertTrue("subject missing", s.contains(subjectHex));
		assertTrue("predicate missing", s.contains(predicateHex));
		assertTrue("object missing", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));
		assertTrue("extra missing", s.contains(" AND something=Something "));
		assertTrue("suffix missing", s.contains(" limit 1"));
	}

	@Test
	public void fullLiteralFindQueryTest() throws TException {

		Quad q = new Quad(graph, subject, predicate, node42);
		QueryPattern qp = new QueryPattern(q);
		String s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue("graph missing", s.contains(graphHex));
		assertTrue("subject missing", s.contains(subjectHex));
		assertTrue("predicate missing", s.contains(predicateHex));
		assertFalse("object found", s.contains(objectHex));
		assertTrue(ColumnName.I+" found", s.contains(ColumnName.I+"=42"));
	}

	@Test
	public void singleAnyLiteralFindQueryTest() throws TException {

		Quad q = new Quad(graph, subject, Node.ANY, node42);
		QueryPattern qp = new QueryPattern(q);
		String s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue("graph missing", s.contains(graphHex));
		assertTrue("subject missing", s.contains(subjectHex));
		assertFalse("predicate found", s.contains(predicateHex));
		assertFalse("object found", s.contains(objectHex));
		assertTrue(ColumnName.I+" found", s.contains(ColumnName.I+"=42"));

		q = new Quad(graph, Node.ANY, predicate, node42);
		qp = new QueryPattern(q);
		s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.POGS"));
		assertFalse("graph found", s.contains(graphHex));
		assertFalse("subject found", s.contains(subjectHex));
		assertTrue("predicate missing", s.contains(predicateHex));
		assertFalse("object found", s.contains(objectHex));
		assertTrue(ColumnName.I+" found", s.contains(ColumnName.I+"=42"));

		q = new Quad(Node.ANY, subject, predicate, node42);
		qp = new QueryPattern(q);
		s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.SPOG"));
		assertFalse("graph found", s.contains(graphHex));
		assertTrue("subject missing", s.contains(subjectHex));
		assertTrue("predicate missing", s.contains(predicateHex));
		assertFalse("object found", s.contains(objectHex));
		assertTrue(ColumnName.I+" found", s.contains(ColumnName.I+"=42"));

	}

	@Test
	public void doubleAnyLiteralFindQueryTest() throws TException {

		Quad q = new Quad(graph, Node.ANY, Node.ANY, node42);
		QueryPattern qp = new QueryPattern(q);
		String s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue("graph missing", s.contains(graphHex));
		assertFalse("subject found", s.contains(subjectHex));
		assertFalse("predicate found", s.contains(predicateHex));
		assertFalse("object found", s.contains(objectHex));
		assertTrue(ColumnName.I+" found", s.contains(ColumnName.I+"=42"));

		q = new Quad(Node.ANY, subject, Node.ANY, node42);
		qp = new QueryPattern(q);
		s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.SPOG"));
		assertFalse("graph found", s.contains(graphHex));
		assertTrue("subject missing", s.contains(subjectHex));
		assertFalse("predicate found", s.contains(predicateHex));
		assertFalse("object found", s.contains(objectHex));
		assertTrue(ColumnName.I+" found", s.contains(ColumnName.I+"=42"));

		q = new Quad(Node.ANY, Node.ANY, predicate, node42);
		qp = new QueryPattern(q);
		s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.POGS"));
		assertFalse("graph found", s.contains(graphHex));
		assertFalse("subject found", s.contains(subjectHex));
		assertTrue("predicate missing", s.contains(predicateHex));
		assertFalse("object found", s.contains(objectHex));
		assertTrue(ColumnName.I+" found", s.contains(ColumnName.I+"=42"));

	}

	@Test
	public void tripleAnyLiteralFindQueryTest() throws TException {

		Quad q = new Quad(Node.ANY, Node.ANY, Node.ANY, node42);
		QueryPattern qp = new QueryPattern(q);
		String s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.GSPO"));
		assertFalse("graph found", s.contains(graphHex));
		assertFalse("subject found", s.contains(subjectHex));
		assertFalse("predicate found", s.contains(predicateHex));
		assertFalse("object found", s.contains(objectHex));
		assertTrue(ColumnName.I+" found", s.contains(ColumnName.I+"=42"));
		assertTrue("graph scan missing", s.contains(" token(graph) >= -9223372036854775808 "));

	}

	@Test
	public void singleAnyFindQueryTest() throws TException {

		Quad q = new Quad(graph, subject, predicate, Node.ANY);
		QueryPattern qp = new QueryPattern(q);
		String s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue("graph missing", s.contains(graphHex));
		assertTrue("subject missing", s.contains(subjectHex));
		assertTrue("predicate found", s.contains(predicateHex));
		assertFalse("object found", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));

		q = new Quad(graph, subject, Node.ANY, object);
		qp = new QueryPattern(q);
		s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.OSGP"));
		assertTrue("graph missing", s.contains(graphHex));
		assertTrue("subject missing", s.contains(subjectHex));
		assertFalse("predicate found", s.contains(predicateHex));
		assertTrue("object missing", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));

		q = new Quad(graph, Node.ANY, predicate, object);
		qp = new QueryPattern(q);
		s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.POGS"));
		assertTrue("graph missing", s.contains(graphHex));
		assertFalse("subject found", s.contains(subjectHex));
		assertTrue("predicate missing", s.contains(predicateHex));
		assertTrue("object missing", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));

		q = new Quad(Node.ANY, subject, predicate, object);
		qp = new QueryPattern(q);
		s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.SPOG"));
		assertFalse("graph found", s.contains(graphHex));
		assertTrue("subject missing", s.contains(subjectHex));
		assertTrue("predicate missing", s.contains(predicateHex));
		assertTrue("object missing", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));

	}

	@Test
	public void doubleAnyFindQueryTest() throws TException {

		Quad q = new Quad(graph, subject, Node.ANY, Node.ANY);
		QueryPattern qp = new QueryPattern(q);
		String s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue("graph missing", s.contains(graphHex));
		assertTrue("subject missing", s.contains(subjectHex));
		assertFalse("predicate found", s.contains(predicateHex));
		assertFalse("object found", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));

		/* this one need a filter */
		q = new Quad(graph, Node.ANY, predicate, Node.ANY);
		qp = new QueryPattern(q);
		s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.POGS"));
		assertFalse("graph found", s.contains(graphHex));
		assertFalse("subject found", s.contains(subjectHex));
		assertTrue("predicate missing", s.contains(predicateHex));
		assertFalse("object found", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));

		q = new Quad(Node.ANY, subject, predicate, Node.ANY);
		qp = new QueryPattern(q);
		s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.SPOG"));
		assertFalse("graph found", s.contains(graphHex));
		assertTrue("subject missing", s.contains(subjectHex));
		assertTrue("predicate missing", s.contains(predicateHex));
		assertFalse("object found", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));

		/*
		 * this one requires a filter
		 */
		q = new Quad(graph, Node.ANY, Node.ANY, object);
		qp = new QueryPattern(q);
		s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.OSGP"));
		assertFalse("graph found", s.contains(graphHex));
		assertFalse("subject found", s.contains(subjectHex));
		assertFalse("predicate found", s.contains(predicateHex));
		assertTrue("object missing", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));

		q = new Quad(Node.ANY, subject, Node.ANY, object);
		qp = new QueryPattern(q);
		s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.OSGP"));
		assertFalse("graph found", s.contains(graphHex));
		assertTrue("subject missing", s.contains(subjectHex));
		assertFalse("predicate found", s.contains(predicateHex));
		assertTrue("object missing", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));

		q = new Quad(Node.ANY, Node.ANY, predicate, object);
		qp = new QueryPattern(q);
		s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.POGS"));
		assertFalse("graph found", s.contains(graphHex));
		assertFalse("subject found", s.contains(subjectHex));
		assertTrue("predicate missing", s.contains(predicateHex));
		assertTrue("object missing", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));

	}

	@Test
	public void tripleAnyFindQueryTest() throws TException {

		Quad q = new Quad(Node.ANY, Node.ANY, Node.ANY, object);
		QueryPattern qp = new QueryPattern(q);
		String s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.OSGP"));
		assertFalse("graph found", s.contains(graphHex));
		assertFalse("subject found", s.contains(subjectHex));
		assertFalse("predicate found", s.contains(predicateHex));
		assertTrue("object missing", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));

		q = new Quad(Node.ANY, Node.ANY, predicate, Node.ANY);
		qp = new QueryPattern(q);
		s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.POGS"));
		assertFalse("graph found", s.contains(graphHex));
		assertFalse("subject found", s.contains(subjectHex));
		assertTrue("predicate missing", s.contains(predicateHex));
		assertFalse("object found", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));

		q = new Quad(Node.ANY, subject, Node.ANY, Node.ANY);
		qp = new QueryPattern(q);
		s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.SPOG"));
		assertFalse("graph found", s.contains(graphHex));
		assertTrue("subject missing", s.contains(subjectHex));
		assertFalse("predicate found", s.contains(predicateHex));
		assertFalse("object found", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));

		q = new Quad(graph, Node.ANY, Node.ANY, Node.ANY);
		qp = new QueryPattern(q);
		s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.GSPO"));
		assertTrue("graph missing", s.contains(graphHex));
		assertFalse("subject found", s.contains(subjectHex));
		assertFalse("predicate found", s.contains(predicateHex));
		assertFalse("object found", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));

	}

	@Test
	public void quadAnyFindQueryTest() throws TException {

		Quad q = new Quad(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
		QueryPattern qp = new QueryPattern(q);
		String s = qp.getFindQuery("test", null, null) + " ";
		assertTrue("table missing", s.contains("test.GSPO"));
		assertFalse("graph found", s.contains(graphHex));
		assertFalse("subject found", s.contains(subjectHex));
		assertFalse("predicate found", s.contains(predicateHex));
		assertFalse("object found", s.contains(objectHex));
		assertFalse(ColumnName.I+" found", s.contains(ColumnName.I.toString()));
		assertTrue("graph scan missing", s.contains(" token(graph) >= -9223372036854775808 "));

	}

	@Test
	public void literalInsertTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, node42);
		QueryPattern qp = new QueryPattern(q);
		String s = qp.getInsertStatement("test");
		String[] lines = s.split("\n");
		assertEquals(6, lines.length);
		assertEquals("BEGIN BATCH", lines[0]);

		assertTrue("table missing", lines[1].contains("test.GSPO"));
		assertTrue("insert columns missing", lines[1].contains("(subject, predicate, object, graph, "+ColumnName.I+")"));
		assertTrue("graphHexValue missing", lines[1].contains(" " + graphHexValue + ", "));
		assertTrue("subjectHexValue missing", lines[1].contains(" " + subjectHexValue + ", "));
		assertTrue("predicateHexValue missing", lines[1].contains(" " + predicateHexValue + ", "));
		assertTrue("node42HexValue missing", lines[1].contains(" " + node42HexValue + ", "));
		assertTrue("42 missing", lines[1].contains("42);"));

		assertTrue("table missing", lines[2].contains("test.OSGP"));
		assertTrue("insert columns missing", lines[2].contains("(subject, predicate, object, graph, "+ColumnName.I+")"));
		assertTrue("graphHexValue missing", lines[2].contains(" " + graphHexValue + ", "));
		assertTrue("subjectHexValue missing", lines[2].contains(" " + subjectHexValue + ", "));
		assertTrue("predicateHexValue missing", lines[2].contains(" " + predicateHexValue + ", "));
		assertTrue("node42HexValue missing", lines[2].contains(" " + node42HexValue + ", "));
		assertTrue("42 missing", lines[2].contains("42);"));

		assertTrue("table missing", lines[3].contains("test.POGS"));
		assertTrue("insert columns missing", lines[3].contains("(subject, predicate, object, graph, "+ColumnName.I+")"));
		assertTrue("graphHexValue missing", lines[3].contains(" " + graphHexValue + ", "));
		assertTrue("subjectHexValue missing", lines[3].contains(" " + subjectHexValue + ", "));
		assertTrue("predicateHexValue missing", lines[3].contains(" " + predicateHexValue + ", "));
		assertTrue("node42HexValue missing", lines[3].contains(" " + node42HexValue + ", "));
		assertTrue("42 missing", lines[3].contains("42);"));

		assertTrue("table missing", lines[4].contains("test.SPOG"));
		assertTrue("insert columns missing", lines[4].contains("(subject, predicate, object, graph, "+ColumnName.I+")"));
		assertTrue("graphHexValue missing", lines[4].contains(" " + graphHexValue + ", "));
		assertTrue("subjectHexValue missing", lines[4].contains(" " + subjectHexValue + ", "));
		assertTrue("predicateHexValue missing", lines[4].contains(" " + predicateHexValue + ", "));
		assertTrue("node42HexValue missing", lines[4].contains(" " + node42HexValue + ", "));
		assertTrue("42 missing", lines[4].contains("42);"));

		assertEquals("APPLY BATCH;", lines[5]);

	}

	@Test
	public void insertTest() throws TException {
		Quad q = new Quad(graph, subject, predicate, object);
		QueryPattern qp = new QueryPattern(q);
		String s = qp.getInsertStatement("test");
		String[] lines = s.split("\n");
		assertEquals(6, lines.length);
		assertEquals("BEGIN BATCH", lines[0]);

		assertTrue("table missing", lines[1].contains("test.GSPO"));
		assertTrue("insert columns missing", lines[1].contains("(subject, predicate, object, graph)"));
		assertTrue("graphHexValue missing", lines[1].contains(" " + graphHexValue + ");"));
		assertTrue("subjectHexValue missing", lines[1].contains(" " + subjectHexValue + ", "));
		assertTrue("predicateHexValue missing", lines[1].contains(" " + predicateHexValue + ", "));
		assertTrue("objectHexValue missing", lines[1].contains(" " + objectHexValue + ", "));
		assertFalse("42 found", lines[1].contains("42);"));

		assertTrue("table missing", lines[2].contains("test.OSGP"));
		assertTrue("insert columns missing", lines[2].contains("(subject, predicate, object, graph)"));
		assertTrue("graphHexValue missing", lines[2].contains(" " + graphHexValue + ");"));
		assertTrue("subjectHexValue missing", lines[2].contains(" " + subjectHexValue + ", "));
		assertTrue("predicateHexValue missing", lines[2].contains(" " + predicateHexValue + ", "));
		assertTrue("objectHexValue missing", lines[2].contains(" " + objectHexValue + ", "));
		assertFalse("42 found", lines[2].contains("42);"));

		assertTrue("table missing", lines[3].contains("test.POGS"));
		assertTrue("insert columns missing", lines[3].contains("(subject, predicate, object, graph)"));
		assertTrue("graphHexValue missing", lines[3].contains(" " + graphHexValue + ");"));
		assertTrue("subjectHexValue missing", lines[3].contains(" " + subjectHexValue + ", "));
		assertTrue("predicateHexValue missing", lines[3].contains(" " + predicateHexValue + ", "));
		assertTrue("objectHexValue missing", lines[3].contains(" " + objectHexValue + ", "));
		assertFalse("42 found", lines[3].contains("42);"));

		assertTrue("table missing", lines[4].contains("test.SPOG"));
		assertTrue("insert columns missing", lines[4].contains("(subject, predicate, object, graph)"));
		assertTrue("graphHexValue missing", lines[4].contains(" " + graphHexValue + ");"));
		assertTrue("subjectHexValue missing", lines[4].contains(" " + subjectHexValue + ", "));
		assertTrue("predicateHexValue missing", lines[4].contains(" " + predicateHexValue + ", "));
		assertTrue("objectHexValue missing", lines[4].contains(" " + objectHexValue + ", "));
		assertFalse("42 found", lines[4].contains("42);"));

		assertEquals("APPLY BATCH;", lines[5]);

	}
}
