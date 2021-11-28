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

package org.apache.jena.cassandra.assembler;

import static org.apache.jena.sparql.util.graph.GraphUtils.getResourceValue;
import static org.apache.jena.sparql.util.graph.GraphUtils.getStringValue;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.Mode;
import org.apache.jena.assembler.assemblers.AssemblerBase;
import org.apache.jena.cassandra.graph.CassandraConnection;
import org.apache.jena.cassandra.graph.GraphCassandra;
import org.apache.jena.cassandra.graph.CassandraConnection.NodeProbeConfig;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.thrift.transport.TTransportException;

import com.datastax.driver.core.Cluster;

/**
 * Assember to construct Models from a Cassandra Cluster and a keyspace.
 *
 * If a graphName is provided it will be used as the model, otherwise the
 * default graph is used.
 *
 *
 */
public class CassandraModelAssembler extends AssemblerBase implements Assembler {

	// Make a model - the default model of the Cassandra dataset
	// [] rdf:type joc:Model ;
	// joc:useCluster "clusterName" ;
	// joc:keyspace "keyspace"

	// Make a named model.
	// [] rdf:type joc:Model ;
	// joc:useCluster "clusterName";
	// joc:keyspace "keyspace"
	// joc:graphName <graphIRI>

	@Override
	public Model open(Assembler a, Resource root, Mode mode) {
		String keyspace = getStringValue(root, VocabCassandra.keyspace);
		String clusterName = getStringValue(root, VocabCassandra.useCluster);
		Resource graphName = getResourceValue(root, VocabCassandra.graphName);

		Cluster cluster = CassandraClusterAssembler.getCluster(root, clusterName);
        NodeProbeConfig nodeProbeConfig = CassandraNodeProbeAssembler.getNodeProbeConfig(root, clusterName );

		try {
            CassandraConnection connection = new CassandraConnection(cluster, nodeProbeConfig);

            Graph g = new GraphCassandra((graphName == null ? null : graphName.asNode()), keyspace, connection);
            return ModelFactory.createModelForGraph(g);
        } catch (TTransportException e) {
            throw new IllegalStateException( "Unable to create CassandraConnection", e );
        }

	}

}
