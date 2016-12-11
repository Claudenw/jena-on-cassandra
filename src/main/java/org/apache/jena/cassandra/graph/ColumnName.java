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
	S("subject", "blob"), P("predicate", "blob"), O("object", "blob"), G("graph", "blob"), 
	I( "obj_idx", "varint");

	private String name;
	private String datatype;

	/**
	 * Constructor.
	 * 
	 * @param name
	 *            The long name of the column
	 */
	ColumnName(String name, String datatype) {
		this.name = name;
		this.datatype = datatype;
	}

	/**
	 * Returns the column long name.
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
		if (datatype.equals( "blob"))
		{
			return String.format( "token(%s) >= %s", this, Long.MIN_VALUE);
		}
		else
		{
			return String.format( "%s >= %s", this, Integer.MIN_VALUE);
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
