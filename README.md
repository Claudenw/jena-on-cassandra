# jena-on-cassandra
An implementation of the Jena storage layer on the Cassandra storage engine.

Create a keyspace on the Cassandra server for each DataSet or collection of graphs.

Create an instance of CassandraConnection.

Use the keyspace name and the CassandraConnection to create a DatasetGraphCassandra or 
GraphCassandra instance.

Once constructed they should work as any normal DatasetGraph or Graph.

## Cassandra Table Design

There are 4 tables created in the Cassandra keyspace. The tables have 4 primary segments identified:
* G = Graph Name
* S = Subject
* P = Predicate
* O = Object

The tables are identified by their column order and are SPOG, PGOS, OSGP, and GSPO.

In addition each table has 3 additional columns with indexes.

* obj_dtype - text - The rdf datatype of the object.
* obj_int - int - the integer value of the object if it is an integer or can be converted to an integer.
* obj_value - text - The string representation of the object value

## Query Design

When the Graph.find(), DatasetGraph.find() or DatasetGraph.findNG() is called the query engine determines which table 
can best answer the query given the g, s, p or o values.

Depending on the value of the o the indexes are added to the primary segments.
