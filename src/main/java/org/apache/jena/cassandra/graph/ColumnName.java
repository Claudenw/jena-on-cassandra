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

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

/**
 * An enumeration that handles columns in a table.
 *
 */
public enum ColumnName {
	S("subject", "blob", 0), P("predicate", "blob", 1), O("object", "blob", 2), G("graph", "blob" ,3), 
	L("obj_lang", "text", 4), D("obj_dtype", "text", 5), I( "obj_int", "varint", -1), V( "obj_value", "blob", 6);

	/* the long name of the column */
	private String name;
	/* the cassandra data type of the column */
	private String datatype;
	/* the query position in the standard query */
	private int queryPos;

	/**
	 * Constructor.
	 * 
	 * @param name
	 *            The long name of the column
	 */
	ColumnName(String name, String datatype, int queryPos) {
		this.name = name;
		this.datatype = datatype;
		this.queryPos = queryPos;
	}
	
	/**
	 * The position of this column in the standard query.
	 * -1 if not included in the standard query.
	 * @return the position fo this column in the standard query.  
	 */
	public int getQueryPos()
	{
		return queryPos;
	}

	/**
	 * Returns the column long name.
	 * @return the column long name.
	 */
	public String toString() {
		return name;
	}

	/**
	 * Return the column id char.
	 * 
	 * @return
	 */
	public char getId() {
		return name.charAt(0);
	}

	/**
	 * Return the column id for the column in the quad. If the quad value is not
	 * defined (null or ANY) then "_" is returned otherwis the standard column
	 * id is returned.
	 * 
	 * @param quad
	 *            THe quad to check.
	 * @return The column id based on the value in the quad.
	 */
	public char getId(Quad quad) {
		return getMatch(quad) == null ? '_' : getId();
	}

	/**
	 * Get the corresponding match node from the quad.
	 * 
	 * Will not return Node.ANY
	 * 
	 * Any node the is specified as Node ANY will be returned as null.
	 * 
	 * @param quad
	 *            The quad to extract the value from.
	 * @return the value or null.
	 */
	public Node getMatch(Quad quad) {
		switch (this) {
		case S:
			return quad.asTriple().getMatchSubject();
		case P:
			return quad.asTriple().getMatchPredicate();
		case O:
		case I:
		case L:
		case D:
			return quad.asTriple().getMatchObject();
		case G:
			return (Node.ANY.equals(quad.getGraph()) || Quad.isUnionGraph(quad.getGraph())) ? null : quad.getGraph();
		default:
			return null;
		}
	}
	
	/**
	 * Get the scan value for a where clause.
	 * @return The scan value string for this column.
	 */
	public String getScanValue()
	{
		switch (this) {
		case S:
		case P:
		case O:
		case G:
		default:
			return String.format( "token(%s) >= %s", this, Long.MIN_VALUE);
			
		case I:
			return String.format( "%s >= %s", this, Integer.MIN_VALUE);
		case L:
		case D:
		case V:
			return String.format( "%s >= %s", this, "a");		
		}
	}
	
	public String getEqualityValue( Object value )
	{
		if (value == null)
		{
			throw new IllegalArgumentException( "value may not be null");
		}
		switch (this) {
		case S:
		case P:
		case O:
		case G:
		case I:
		default:
			return String.format( "%s=%s", this, value);
		case L:
		case D:
		case V:
			return String.format( "%s='%s'", this, value);		
		}
	}

	public String getInsertValue( Object value )
	{
		if (value == null)
		{
			throw new IllegalArgumentException( "value may not be null");
		}
		switch (this) {
		case S:
		case P:
		case O:
		case G:
		case I:
		default:
			return String.format( "%s", value);
		case L:
		case D:
		case V:
			return String.format( "'%s'", value);		
		}
	}
	/**
	 * The text to create the column.
	 * @return the column definition for construction.
	 */
	public String getCreateText() {
		return String.format( "%s %s", name, datatype);
	}
}
