# jena-on-cassandra
An implementation of the Jena storage layer on the Cassandra storage engine.

To use the code create an instance of CassandraConnection  and then use that to create a DatasetGraphCassandra or 
GraphCassandra object.  When you create either of those objects you have to specify the keyspace that you want to use.

Once constructed they should work as any normal Graph or DatasetGraph would.

