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
import org.apache.jena.assembler.exceptions.AssemblerException;
import org.apache.jena.cassandra.graph.CassandraConnection;
import org.apache.jena.cassandra.graph.GraphCassandra;
import org.apache.jena.query.ARQ;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.util.Symbol;

import com.datastax.driver.core.Cluster;

public class CassandraGraphAssembler extends AssemblerBase implements Assembler {

	// Make a model - the default model of the Cassandra dataset
    // [] rdf:type joc:Graph ;
    //    joc:useCluster "clusterName" ;
	//    joc:keyspace "keyspace"
    
    // Make a named model.
    // [] rdf:type joc:Graph ;
    //    joc:useCluster "clusterName";
	//    joc:keyspace "keyspace"
    //    joc:graphName <graphIRI>
	
	@Override
	public Object open(Assembler a, Resource root, Mode mode) {
        String keyspace = getStringValue(root, VocabCassandra.keyspace) ;
        String clusterName = getStringValue(root, VocabCassandra.useCluster) ;
        Resource graphName = getResourceValue(root, VocabCassandra.graphName) ;
        
        Cluster cluster = null;
        Symbol symbol = Symbol.create(String.format( "%s/%s", VocabCassandra.Cluster.getURI(),
        		clusterName)) ;
        
        Object o = ARQ.getContext().get(symbol);
        if (o instanceof Cluster)
        {
        	cluster = (Cluster)o;
        } else {
        	throw new AssemblerException(root, String.format( "%s is not a valid cluster name", clusterName));
        }
        
        CassandraConnection connection = new CassandraConnection( cluster );
        
        return new GraphCassandra( (graphName==null?null:graphName.asNode()), keyspace, connection );
        
	}

}
