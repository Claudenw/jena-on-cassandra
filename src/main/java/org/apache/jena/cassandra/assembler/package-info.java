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


/**
 * 
 * Assembler code to create object necessary to communicate with the the Cassandra servers.
 * 
 * <b>Make a cluster</b>
 * 
 * <pre>
 * [] rdf:type joc:Cluster ;
 *    joc:name "clustername" ;   # This must be unique among all clusters
 *    joc:address url ; 		 # One or more URLs may be provided. 
 *    joc:port port;             # Zero or 1 port may be specified.
 *    joc:compression "snappy" ; # Zero or 1 compression may be specified.                          
 *    joc:credentials [          # Zero or 1 credentials
 *        joc:user "username" ;  # User name to log in with
 *        joc:password "passeord"; # Password  
 *        ];
 *    joc:metrics "true"		 # Zero or 1 flags.
 *    joc:ssl "true"			 # Zero or 1 flags.
 *    </pre>
 *    
 * The compression value must be one of the the Cassandra Protocol Options Compression enum values.
 * 
 * @see com.datastax.driver.core.ProtocolOptions.Compression
 * 
 * The flags need only exist the values do not matter.
 * 
 * <b>Make a dataset</b>
 * 
 * <pre>
 * [] rdf:type joc:Dataset ;
 *    joc:useCluster "clusterName" ;
 *    joc:keyspace "keyspace"
 *  </pre>
 *  
 * The useCluster option must be the name specified in a Cluster.
 * 
 * <b>Make a model</b>
 * 
 * <pre>
 * [] rdf:type joc:Model ;
 *    joc:useCluster "clusterName";
 *    joc:keyspace "keyspace"
 *    joc:graphName &lt;graphIRI&gt;  
 * </pre>	
 * 
 * The graphName is optional, not specifying the graph is the same as urn:x-arq:UnionGraph. 
 * Other valid options are:
 * <ul>
 * <li>urn:x-arq:DefaultGraph -- The name for the default graph in the keyspace</ul>
 * <li>urn:x-arq:UnionGraph -- A union of all the graphs in the keyspace.</li>
 * </ul>
 * 
 */
package org.apache.jena.cassandra.assembler;

