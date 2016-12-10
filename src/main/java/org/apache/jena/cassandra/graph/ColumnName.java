package org.apache.jena.cassandra.graph;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

/**
 * An enumeration that handles columns in a table.
 *
 */
public enum ColumnName {
	S("subject"), P("predicate"), O("object"), G("graph");

	private String name;

	/**
	 * Constructor.
	 * 
	 * @param name
	 *            The long name of the column
	 */
	ColumnName(String name) {
		this.name = name;
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
			return quad.asTriple().getMatchObject();
		case G:
			return (Node.ANY.equals(quad.getGraph()) || Quad.isUnionGraph(quad.getGraph())) ? null : quad.getGraph();
		default:
			return null;
		}
	}

}
