# jena-on-cassandra
An implementation of the Jena storage layer on the Cassandra storage engine.

Create a keyspace on the Cassandra server for each DataSet or collection of graphs.

Create an instance of CassandraConnection.

Use the keyspace name and the CassandraConnection to create a DatasetGraphCassandra or 
GraphCassandra instance.

Once constructed they should work as any normal DatasetGraph or Graph.

