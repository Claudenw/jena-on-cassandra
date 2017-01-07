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

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.tdb.assembler.DatasetAssemblerTDB;
import org.apache.jena.tdb.assembler.TDBGraphAssembler;

public class VocabCassandra {
	
	private static final String NS = "https://jena.apache.org/jena-on-cassandra#" ; 
	
	public static final Resource Cluster = ResourceFactory.createResource( NS+"Cluster"); 

	public static final Property contactPoint = ResourceFactory.createProperty( NS, "contact");
	public static final Property address = ResourceFactory.createProperty( NS, "address");
	public static final Property port = ResourceFactory.createProperty( NS, "port");
	public static final Property compression = ResourceFactory.createProperty( NS, "compression");
	public static final Property credentials = ResourceFactory.createProperty( NS, "credentials");
	public static final Property user = ResourceFactory.createProperty( NS, "user");
	public static final Property password = ResourceFactory.createProperty( NS, "password");
	public static final Property metrics = ResourceFactory.createProperty( NS, "metrics");
	public static final Property ssl = ResourceFactory.createProperty( NS, "ssl");
	public static final Property name = ResourceFactory.createProperty( NS, "name");
	
	public static final Resource Dataset = ResourceFactory.createResource( NS+"Dataset");

	public static final Property useCluster = ResourceFactory.createProperty( NS, "useCluster");
	public static final Property keyspace = ResourceFactory.createProperty( NS, "keyspace");
	
	public static final Resource Model = ResourceFactory.createResource( NS+"Model");
	public static final Property graphName = ResourceFactory.createProperty( NS, "graphName");

	private static boolean initialized = false ; 
    
    static { init() ; }
    
    static synchronized public void init() {
        if ( initialized )
            return;
        initialized = true;
        AssemblerUtils.registerDataset(Dataset, new CassandraDatasetAssembler());
        AssemblerUtils.registerModel(Model, new CassandraModelAssembler());
        AssemblerUtils.registerAssembler(null,Cluster, new CassandraClusterAssembler());
    }
}
