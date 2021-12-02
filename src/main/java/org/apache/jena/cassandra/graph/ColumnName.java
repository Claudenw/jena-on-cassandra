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

import java.math.BigDecimal;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.datatypes.xsd.impl.XSDBaseNumericType;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.thrift.TException;

/**
 * An enumeration that handles columns in a table.
 *
 */
public enum ColumnName {
    S("subject", ColumnType.blob, 0), P("predicate", ColumnType.blob, 1),
    O("object", ColumnType.blob, 2), G("graph", ColumnType.blob ,3),
    L("obj_lang", ColumnType.text, 4), D("obj_dtype", ColumnType.text, 5),
    I( "obj_int", ColumnType.varint, -1), V( "obj_value", ColumnType.text, 6);

    public enum ColumnType {
        blob, text, varint
    }

    private static final Log LOG = LogFactory.getLog(ColumnName.class);

    /* the long name of the column -- first character must be unique across all column names*/
    private String name;
    /* the data type of the column */
    private ColumnType datatype;
    /* the column position in the standard query -1 = not included.*/
    private int queryPos;

    /**
     * Constructor.
     *
     * @param name
     *            The long name of the column
     * @param datatype The data type for the column
     * @param queryPos the column position in the standard query.
     */
    ColumnName(String name, ColumnType datatype, int queryPos) {
        this.name = name;
        this.datatype = datatype;
        this.queryPos = queryPos;
    }

    /**
     * The position of this column in the standard query.
     * -1 if not included in the standard query.
     * @return the position for this column in the standard query.
     */
    public int getQueryPos()
    {
        return queryPos;
    }

    /**
     * Returns the column long name.
     * @return the column long name.
     */
    @Override
    public String toString() {
        return name;
    }

    /**
     * Return the column id char.
     *
     * @return the first character of the name
     */
    public char getId() {
        return name.charAt(0);
    }

    /**
     * Return the column id for the column in the quad. If the quad value is not
     * defined (null or ANY) then "_" is returned otherwise the standard column
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
    public String getScanValue( Object value )
    {
        switch (this.datatype) {
        case blob:
        default:
            if (value == null)
            {
                return String.format( "token(%s) >= %s", this, Long.MIN_VALUE);
            }
            else
            {
                return String.format( "token(%s) = token(%s)", this, value);
            }

        case varint:
            return String.format( "%s >= %s", this, Integer.MIN_VALUE);
        case text:
            return String.format( "%s >= %s", this, "''");
        }
    }

    /**
     * The string to add to a query for an equality check.
     * @param connection The connection to use
     * @param value the object value.
     * @return the string for the query
     */
    public String getEqualityValue( CassandraConnection connection, Object value )
    {
        if (value == null)
        {
            throw new IllegalArgumentException( "value may not be null");
        }
        return String.format( "%s=%s", this, getInsertValue( connection, value ));
    }

    /**
     * The string to add an insert values statement for this column.
     * @param connection The connection to use
     * @param value the value of the object.
     * @return The value string.
     */
    public String getInsertValue( CassandraConnection connection, Object value )
    {
        if (value == null)
        {
            throw new IllegalArgumentException( "value may not be null");
        }
        switch (this.datatype) {
        case blob:
        case varint:
        default:
            try {
                return
                        (value instanceof Node) ? connection.valueOf( (Node)value ) :
                            value.toString();
            } catch (TException e) {
                throw new IllegalStateException(String.format("Unable to encode %s",value), e );
            }
        case text:
            return String.format( "'%s'", value.toString().replaceAll("'", "''"));
        }
    }

    /**
     * The text to create the column.
     * @return the column definition for construction.
     */
    public String getCreateText() {
        return String.format( "%s %s", name, datatype);
    }

    /**
     * Get the value object for this column based on the the data in the quad.
     *
     * @param quad The quad data.
     * @return The object for this column.
     */
    public Object getValue(Quad quad)
    {
        Node n;
        switch (this) {
        case S:
        case P:
        case G:
        case O:
            return getMatch(quad);
        case V:
            n = ColumnName.O.getMatch(quad);
            if (n != null) {
                if (n.isLiteral()) {
                    return n.getLiteralLexicalForm();
                }
            }
            break;
        case I:
            n = ColumnName.O.getMatch(quad);
            if (n != null) {
                if (n.isLiteral() && n.getLiteralDatatype() instanceof XSDBaseNumericType) {
                    return new BigDecimal(n.getLiteral().getLexicalForm());
                }
            }
            break;
        case L:
            n = ColumnName.O.getMatch(quad);
            if (n != null) {
                if (n.isLiteral() && StringUtils.isNotEmpty(n.getLiteralLanguage())) {
                    return n.getLiteralLanguage();
                }
            }
            break;
        case D:
            n = ColumnName.O.getMatch(quad);
            if (n != null) {
                if (n.isLiteral()) {
                    return n.getLiteralDatatypeURI();
                }
            }
            break;
        default:
            LOG.warn("Unhandled column type: " + this);
        }
        return null;
    }

}
