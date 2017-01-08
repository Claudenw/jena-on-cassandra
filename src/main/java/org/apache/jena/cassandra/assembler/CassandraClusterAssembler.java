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

import static org.apache.jena.sparql.util.graph.GraphUtils.getStringValue;
import static org.apache.jena.sparql.util.graph.GraphUtils.exactlyOneProperty;
import static org.apache.jena.sparql.util.graph.GraphUtils.multiValueString;

import org.apache.cassandra.io.util.FileUtils;

import static org.apache.jena.sparql.util.graph.GraphUtils.atmostOneProperty;
import static org.apache.jena.sparql.util.graph.GraphUtils.getResourceValue;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.Mode;
import org.apache.jena.assembler.assemblers.AssemblerBase;
import org.apache.jena.assembler.exceptions.AssemblerException;
import org.apache.jena.query.ARQ;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.sparql.util.NotUniqueException;
import org.apache.jena.sparql.util.Symbol;
import org.apache.jena.vocabulary.RDF;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions.Compression;

public class CassandraClusterAssembler extends AssemblerBase implements Assembler {

	// Make a cluster
    // [] rdf:type joc:Cluster ;
	//    joc:name "clustername" ;
    //    joc:address url ;
	//    joc:port port ;
	//    joc:compression "snappy | lz4 "
	//    joc:credentials [ joc:user "username" ;
	//	                    joc:password "passeord" ];
	//    joc:metrics "true"
	//    joc:ssl "true"

	
	@Override
	public Cluster open(Assembler a, Resource root, Mode mode) {
		if (! exactlyOneProperty(root, VocabCassandra.name )) {
			throw new AssemblerException( root, String.format( "%s must be specified", VocabCassandra.name.getLocalName()));
		}
		
        String name = getStringValue(root, VocabCassandra.name) ;
        
        Cluster.Builder builder = Cluster.builder().withClusterName(name);
        
        for (String address : multiValueString(root, VocabCassandra.address))
        {
        	builder.addContactPoint(address);
        }
        if (builder.getContactPoints().isEmpty())
        	{
        	throw new AssemblerException( root, String.format( "At least on %s must be specified", VocabCassandra.address.getLocalName()));
        	}
        	
        String port = getStringValue( root, VocabCassandra.port);
        if (port != null)
        {
        	try {
        	int p = Integer.valueOf(port);
        	builder.withPort(p);
        	}
        	catch (NumberFormatException e)
        	{
            	throw new AssemblerException( root, String.format( "Port (%s) must be a number", port));        		
        	}
        }
        
        String compression = getStringValue( root, VocabCassandra.compression);
        if (compression != null)
        {
        	Compression comp = Compression.valueOf(compression.toUpperCase());
        	if (comp == null)
        	{
            	throw new AssemblerException( root, String.format( "Compression (%s) must be 'snappy' or 'lz4' or not specified", port));        		
    
        	}
        	else {
        	builder.withCompression( comp );
        	}
        }
        
        if (! atmostOneProperty( root, VocabCassandra.credentials))
        {
			throw new AssemblerException( root, String.format( "At most one %s may be specified", VocabCassandra.credentials.getLocalName()));
        } 
        
        Resource credentials = getResourceValue( root, VocabCassandra.credentials);
        if (credentials != null)
        {
        	String username = getStringValue( credentials, VocabCassandra.user);
        	if (username == null)
        	{
    			throw new AssemblerException( root, String.format( "If %s is specified %s must be specified", VocabCassandra.credentials.getLocalName(), VocabCassandra.user.getLocalName()));
        	}
        	String password = getStringValue( credentials, VocabCassandra.password);
        	if (password == null)
        	{
    			throw new AssemblerException( root, String.format( "If %s is specified %s must be specified", VocabCassandra.credentials.getLocalName(), VocabCassandra.password.getLocalName()));
        	}
        	builder.withCredentials(username, password);
        }
        
        RDFNode metrics = getNode( root, VocabCassandra.metrics);
        if (metrics == null)
        {
        	builder.withoutMetrics();
        }
        
        RDFNode ssl = getNode( root, VocabCassandra.ssl);
        if (ssl == null)
        {
        	builder.withSSL();
        }
      
        return register(builder.build(), name);
	}
	
	private static Cluster register(Cluster cluster, String name) {
        Symbol symbol = Symbol.create(String.format( "%s/%s", VocabCassandra.Cluster.getURI(),
        		name)) ;
        
        ARQ.getContext().set(symbol, cluster);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                cluster.close();
        }}));
        
        return cluster;
	}
	
	private RDFNode getNode( Resource r, Property p)
	{
		if ( !atmostOneProperty(r, p) )
            throw new NotUniqueException(r, p) ;
        Statement s = r.getProperty(p) ;
        if ( s == null )
            return null ;
        return s.getObject();
	}
	
	public static Cluster getCluster( Resource root, String clusterName )
	{
       
        Symbol symbol = Symbol.create(String.format( "%s/%s", VocabCassandra.Cluster.getURI(),
        		clusterName)) ;
        
        Object o = ARQ.getContext().get(symbol);
        if (o == null)
        {
        	Model model = root.getModel();
        	for (Resource r : model.listResourcesWithProperty( RDF.type, VocabCassandra.Cluster).toList())
    		{
        		if (r.hasLiteral( VocabCassandra.name, clusterName))
        		{
        			o = Assembler.general.open(r);
        			break;
        		}
    		}
        }
        
        if (o != null && o instanceof Cluster)
        {
        	return (Cluster)o;
        } else {
        	throw new AssemblerException(root, String.format( "%s is not a valid cluster name", clusterName));
        }

	}
	
	public static Cluster getCluster( String clusterName, String contactPoint, int port )
	{
		Symbol symbol = Symbol.create(String.format( "%s/%s", VocabCassandra.Cluster.getURI(),
        		clusterName)) ;
        
        Object o = ARQ.getContext().get(symbol);
        if (o != null && o instanceof Cluster)
        {
        	return (Cluster)o;
        }
        
        return register( Cluster.builder().addContactPoint(contactPoint).withPort(port).build(),
				clusterName);
        
	}

}
