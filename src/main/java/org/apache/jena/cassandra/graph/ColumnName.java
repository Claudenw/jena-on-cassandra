package org.apache.jena.cassandra.graph;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

public enum ColumnName {
	S("subject"), P("predicate"), O("object"), G("graph");
	
	private String name;
	ColumnName(String name) {
		this.name=name;
	}
	
	public String toString()
	{
		return name;
	}
	
	public char getId()
	{
		return name.charAt(0);
	}
	
	public char getId(Quad quad)
	{
		return getMatch(quad)==null?'_':getId();
	}
	
	public char getChar() {
		return super.toString().charAt(0);
	}
	
	/**
	 * Get the node for the match type.
	 * 
	 * A column name is one of "subject", "predicate", "object" or "graph".
	 * 
	 * Will not return Node.ANY.
	 * 
	 * @param columnName The column name to find.
	 * @return the value or null.  
	 */
	public Node getMatch(Quad quad) {
		switch (this)
		{
		case S:
			return quad.asTriple().getMatchSubject();
		case P:
			return quad.asTriple().getMatchPredicate();
		case O:
			return quad.asTriple().getMatchObject();
		case G:
			return (Node.ANY.equals(quad.getGraph()) || Quad.isUnionGraph(quad.getGraph()))? null : quad.getGraph();
			default:
				return null;
		}
	}
	
}

