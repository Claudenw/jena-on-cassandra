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
import static org.apache.jena.sparql.util.graph.GraphUtils.atmostOneProperty;
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

/**
 * An assembler for the Cassandra Clusters. This assembler ensures that the
 * loaded clusters are shut down when the JVM exits.
 *
 * Clusters should only be constructed once. To ensure this the constructed
 * objects are placed in the ARQ context.
 *
 */
public class CassandraClusterAssembler extends AssemblerBase implements Assembler {

    //
    // Make a cluster
    // [] rdf:type joc:Cluster ;
    // joc:name "clustername" ;
    // joc:address url ;
    // joc:port port ;
    // joc:compression "snappy | lz4 "
    // joc:credentials [ joc:user "username" ;
    // joc:password "password" ];
    // joc:metrics "true"
    // joc:ssl "true"

    @Override
    public Cluster open(Assembler a, Resource root, Mode mode) {

        String name = CassandraOptionsParser.parseName(root);

        Cluster.Builder builder = Cluster.builder().withClusterName(name);

        CassandraOptionsParser.parseAddress(root, builder::addContactPoints );
        if (builder.getContactPoints().isEmpty()) {
            throw new AssemblerException(root,
                    String.format("At least on %s must be specified", VocabCassandra.address.getLocalName()));
        }

        CassandraOptionsParser.parsePort(root, builder::withPort);


        String compression = getStringValue(root, VocabCassandra.compression);
        if (compression != null) {
            Compression comp = Compression.valueOf(compression.toUpperCase());
            if (comp == null) {
                throw new AssemblerException(root,
                        String.format("Compression (%s) must be 'snappy' or 'lz4' or not specified", compression));

            } else {
                builder.withCompression(comp);
            }
        }

        CassandraOptionsParser.parseCredentials(root, builder::withCredentials);

        String metrics = getStringValue(root, VocabCassandra.metrics);
        if (metrics != null && ! Boolean.valueOf(metrics))
        {
            builder.withoutMetrics();
        }

        if (CassandraOptionsParser.parseSSL(root)) {
            builder.withSSL();
        }

        return register(builder.build(), name);
    }

    private static Cluster register(Cluster cluster, String name) {
        Symbol symbol = Symbol.create(String.format("%s/%s", VocabCassandra.Cluster.getURI(), name));

        ARQ.getContext().set(symbol, cluster);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                cluster.close();
            }
        }));

        return cluster;
    }

    private RDFNode getNode(Resource r, Property p) {
        if (!atmostOneProperty(r, p))
            throw new NotUniqueException(r, p);
        Statement s = r.getProperty(p);
        if (s == null)
            return null;
        return s.getObject();
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
    public static Cluster getCluster(Resource root, String clusterName) {

        Symbol symbol = Symbol.create(String.format("%s/%s", VocabCassandra.Cluster.getURI(), clusterName));

        Object o = ARQ.getContext().get(symbol);
        if (o == null) {
            Model model = root.getModel();
            for (Resource r : model.listResourcesWithProperty(RDF.type, VocabCassandra.Cluster).toList()) {
                if (r.hasLiteral(VocabCassandra.name, clusterName)) {
                    o = Assembler.general.open(r);
                    break;
                }
            }
        }

        if (o != null && o instanceof Cluster) {
            return (Cluster) o;
        } else {
            throw new AssemblerException(root, String.format("%s is not a valid cluster name", clusterName));
        }

    }


    public static int getThreadCount(Resource root, String clusterName) {
        Model model = root.getModel();
        for (Resource r : model.listResourcesWithProperty(RDF.type, VocabCassandra.Cluster).toList()) {
            if (r.hasLiteral(VocabCassandra.name, clusterName)) {
                return CassandraOptionsParser.parseThreadCount(root, 5);
            }
        }
        return 5;
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
    public static Cluster getCluster(String clusterName, String contactPoint, int port) {
        Symbol symbol = Symbol.create(String.format("%s/%s", VocabCassandra.Cluster.getURI(), clusterName));

        Object o = ARQ.getContext().get(symbol);
        if (o != null && o instanceof Cluster) {
            return (Cluster) o;
        }

        return register(Cluster.builder().addContactPoint(contactPoint).withPort(port).build(), clusterName);

    }

}
