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
import static org.apache.jena.sparql.util.graph.GraphUtils.atmostOneProperty;
import static org.apache.jena.sparql.util.graph.GraphUtils.getResourceValue;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.Mode;
import org.apache.jena.assembler.assemblers.AssemblerBase;
import org.apache.jena.assembler.exceptions.AssemblerException;
import org.apache.jena.cassandra.graph.CassandraConnection.NodeProbeConfig;
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

/**
 * An assembler for the Cassandra Clusters. This assembler ensures that the
 * loaded clusters are shut down when the JVM exits.
 *
 * Clusters should only be constructed once. To ensure this the constructed
 * objects are placed in the ARQ context.
 *
 */
public class CassandraNodeProbeAssembler extends AssemblerBase implements Assembler {

	//
	// Make a cluster
	// [] rdf:type joc:Cluster ;
	// joc:name "clustername" ;
	// joc:address url ;
	// joc:port port ;
	// joc:compression "snappy | lz4 "
	// joc:credentials [
    //      joc:user "username" ;
	//      joc:password "password"
    // ];
	// joc:metrics "true"
	// joc:ssl "true"
    // joc:jmx [
    //      joc:credentials [
    //          joc:user "username" ;
    //          joc:password "password"
    //      ];
    //      joc:port port ;
    //      joc:address url ;
    // ];

	@Override
	public NodeProbeConfig open(Assembler a, Resource root, Mode mode) {

	    NodeProbeConfig config = new NodeProbeConfig();

	    String name = CassandraOptionsParser.parseName(root);

	    if (!atmostOneProperty(root, VocabCassandra.jmx)) {
            throw new AssemblerException(root,
                    String.format("At most one %s may be specified", VocabCassandra.jmx.getLocalName()));
        }

        Resource jmx = getResourceValue(root, VocabCassandra.jmx);
        if (jmx != null) {
            CassandraOptionsParser.parseAddress(jmx, config::addContactPoints);
            CassandraOptionsParser.parsePort(jmx, config::withPort);
            CassandraOptionsParser.parseCredentials(jmx, config::withCredentials);
        }
        if (!config.hasContactPoints()) {
            CassandraOptionsParser.parseAddress(root, config::addContactPoints);
        }
	    if (!config.hasCredentials()) {
	        CassandraOptionsParser.parseCredentials(root, config::withCredentials);
	    }

		return register(config, name, CassandraOptionsParser.parseSSL(root));
	}

	private static NodeProbeConfig register(NodeProbeConfig nodeProbeConfig, String name, boolean withSSL) {
	    if (nodeProbeConfig.isValid(withSSL)) {
    		Symbol symbol = Symbol.create(String.format("%s/%s", VocabCassandra.NodeProbeConfig.getURI(), name));
    		ARQ.getContext().set(symbol, nodeProbeConfig);
    		return nodeProbeConfig;
	    }
	    return null;
	}

	/**
     * Get cluster named "clusterName" or build it from the root.
     *
     * If a cluster with the cluster name has already been loaded return it,
     * otherwise build it from the model attached to the reasource.
     *
     * @param root
     *            The root resource for the building of the cluster.
     * @param clusterName
     *            The cluster name
     * @return The cluster.
     */
    public static NodeProbeConfig getNodeProbeConfig(Resource root, String clusterName) {

        Symbol symbol = Symbol.create(String.format("%s/%s", VocabCassandra.NodeProbeConfig.getURI(), clusterName));

        Object o = ARQ.getContext().get(symbol);
        if (o == null) {
            Model model = root.getModel();
            CassandraNodeProbeAssembler assembler = new CassandraNodeProbeAssembler();
            for (Resource r : model.listResourcesWithProperty(RDF.type, VocabCassandra.Cluster).toList()) {
                if (r.hasLiteral(VocabCassandra.name, clusterName)) {
                    o = assembler.open(r);
                    break;
                }
            }
        }

        return (NodeProbeConfig) o;
    }

    /**
     * Get a cluster with the specified name.
     *
     * If a cluster with the cluster name has already been loaded return it,
     * otherwise build it from the contactPoint and port.
     *
     * @param clusterName
     *            the cluster name.
     * @param contactPoint
     *            the contactPoint for a new cluster
     * @param port
     *            the port for the new cluster.
     * @return The Cluster
     */
    public static NodeProbeConfig getNodeProbeConfig(String clusterName, String contactPoint, int port, boolean withSSL) {
        Symbol symbol = Symbol.create(String.format("%s/%s", VocabCassandra.NodeProbeConfig.getURI(), clusterName));

        Object o = ARQ.getContext().get(symbol);
        if (o != null && o instanceof NodeProbeConfig) {
            return (NodeProbeConfig) o;
        }

        NodeProbeConfig npc = new NodeProbeConfig();
        npc.addContactPoints(contactPoint);


        return register(npc, clusterName, withSSL );

    }


	private RDFNode getNode(Resource r, Property p) {
		if (!atmostOneProperty(r, p))
			throw new NotUniqueException(r, p);
		Statement s = r.getProperty(p);
		if (s == null)
			return null;
		return s.getObject();
	}


}
