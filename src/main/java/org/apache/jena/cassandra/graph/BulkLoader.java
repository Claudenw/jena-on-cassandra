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
package org.apache.jena.cassandra.graph;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.cassandra.assembler.CassandraNodeProbeAssembler;
import org.apache.jena.cassandra.assembler.VocabCassandra;
import org.apache.jena.cassandra.graph.CassandraConnection.NodeProbeConfig;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;
import org.apache.thrift.transport.TTransportException;

import com.datastax.driver.core.Cluster;

/**
 * Class to bulk load data.
 *
 * Options must be prefixed by dash (-)
 * <dl>
 * <dt>addr</dt>
 * <dd>The server address for the Cassandra server. May occur more than
 * once</dd>
 * <dt>port</dt>
 * <dd>The port for the Cassandra server. Optional, default is 9042.</dd>
 * <dt>comp</dt>
 * <dd>The compression to use. Optional. Valid values are defined by the
 * Cassandra Protocol Options Compression enum values. "snappy" and "lz4" are
 * known good values.</dd>
 * <dt>user</dt>
 * <dd>The user id to use to login to the server. Optional.</dd>
 * <dt>pwd</dt>
 * <dd>The password to use to login to the server. Optional.</dd>
 * <dt>metrics</dt>
 * <dd>Turn metrics on/off. Optional. Values must be "true" or "false". Default
 * is true.</dd>
 * <dt>ssl</dt>
 * <dd>Turn SSL on/off. Optional. Values must be "true" or "false". Default is
 * false.</dd>
 * <dt>keyspace</dt>
 * <dd>The keyspace within th Cassandra server to store tables in.</dd>
 * </dl>
 *
 * All other command line options are considered to be URLs to load data from.
 *
 * The BulkLoader will load data in parallel from up to 4 URLs at a time.
 */
public class BulkLoader {

	//
	// Make a cluster
	// [] rdf:type joc:Cluster ;
	// joc:name "clustername" ;
	// joc:address url ;
	// joc:port port ;
	// joc:compression "snappy | lz4 "
	// joc:credentials [ joc:user "username" ;
	// joc:password "passeord" ];
	// joc:metrics "true"
	// joc:ssl "true"
	private static String ADDR = "addr";
	private static String PORT = "port";
	private static String COMP = "comp";
	private static String USER = "user";
	private static String PWD = "pwd";
	private static String METRICS = "metrics";
	private static String SSL = "ssl";
	private static String KEYSPACE = "keyspace";
	private static String JMX_ADDR = "jmx."+ADDR;
    private static String JMX_PORT = "jmx."+PORT;
    private static String JMX_USER = "jmx."+USER;
    private static String JMX_PWD = "jmx."+PWD;



	/**
	 * Main executable.
	 *
	 * Options must be prefixed by dash (-)
	 * <dl>
	 * <dt>addr</dt>
	 * <dd>The server address for the Cassandra server. May occur more than
	 * once</dd>
	 * <dt>port</dt>
	 * <dd>The port for the Cassandra server. Optional, default is 9042.</dd>
	 * <dt>comp</dt>
	 * <dd>The compression to use. Optional. Valid values are defined by the
	 * Cassandra Protocol Options Compression enum values. "snappy" and "lz4"
	 * are known good values.</dd>
	 * <dt>user</dt>
	 * <dd>The user id to use to login to the server. Optional.</dd>
	 * <dt>pwd</dt>
	 * <dd>The password to use to login to the server. Optional.</dd>
	 * <dt>metrics</dt>
	 * <dd>Turn metrics on/off. Optional. Values must be "true" or "false".
	 * Default is true.</dd>
	 * <dt>ssl</dt>
	 * <dd>Turn SSL on/off. Optional. Values must be "true" or "false". Default
	 * is false.</dd>
	 * <dt>keyspace</dt>
	 * <dd>The keyspace within th Cassandra server to store tables in.</dd>
	 * </dl>
	 *
	 * All other command line options are considered to be URLs to load data
	 * from. Triple data is loaded into the default graph, quad data is loaded
	 * into the graph specified by the quad.
	 *
	 * @param args
	 *            The arguments.
	 * @throws TTransportException
	 * @throws IllegalArgumentException
	 *             if an argument is not understood.
	 */
	public static void main(String[] args) throws TTransportException {
		Resource cfg = ModelFactory.createMemModelMaker().createDefaultModel().createResource();
		cfg.addProperty(RDF.type, VocabCassandra.Cluster);
		cfg.addProperty(VocabCassandra.name, "BulkLoader");
		List<String> urls = new ArrayList<String>();
		String keyspace = null;

		int i = 0;

		while (i < args.length) {
			if (args[i].startsWith("-")) {
				String name = args[i].substring(1).toLowerCase();
				i++;
				if (i >= args.length) {
					throw new IllegalArgumentException(String.format("-%s requires and argument", name));
				}
				if (ADDR.equals(name)) {
					cfg.addLiteral(VocabCassandra.address, args[i]);
				} else if (PORT.equals(name)) {
					cfg.addLiteral(VocabCassandra.port, args[i]);
				} else if (COMP.equals(name)) {
					cfg.addLiteral(VocabCassandra.compression, args[i]);
				} else if (USER.equals(name)) {
				    getCred(cfg).addLiteral(VocabCassandra.user, args[i]);
				} else if (PWD.equals(name)) {
				    getCred(cfg).addLiteral(VocabCassandra.password, args[i]);
				} else if (METRICS.equals(name)) {
					cfg.addLiteral(VocabCassandra.metrics, args[i]);
				} else if (SSL.equals(name)) {
					cfg.addLiteral(VocabCassandra.ssl, args[i]);

				} else if (KEYSPACE.equals(name)) {
					keyspace = args[i];
				} else if (JMX_ADDR.equals(name)) {
				    getJmx(cfg).addLiteral(VocabCassandra.address, args[i]);
				} else if (JMX_PORT.equals(name)) {
				    getJmx(cfg).addLiteral(VocabCassandra.port, args[i]);
				} else if (JMX_USER.equals(name)) {
				    getCred( getJmx(cfg) ).addLiteral(VocabCassandra.user, args[i]);
				} else if (JMX_PWD.equals(name)) {
                getCred( getJmx(cfg) ).addLiteral(VocabCassandra.password, args[i]);
				} else {
					throw new IllegalArgumentException(String.format("unknown options -%s", name));
				}
			} else {
				urls.add(args[i]);
			}
			i++;
		}

		if (keyspace == null) {
			throw new IllegalArgumentException("-keyspace must be defined");
		}
		Cluster cluster = (Cluster) Assembler.general.open(cfg);
		CassandraNodeProbeAssembler nodeProbeAssembler = new CassandraNodeProbeAssembler();
        NodeProbeConfig nodeProbeConfig = (NodeProbeConfig) nodeProbeAssembler.open(cfg);

		CassandraConnection connection = new CassandraConnection(cluster, nodeProbeConfig);
		execute(connection, keyspace, urls);
	}

	private static Resource getProperty(Resource cfg, Property prop ) {
        Statement stmt = cfg.getProperty(prop);
        if (stmt != null) {
            return stmt.getObject().asResource();
        }
        Resource jmx = cfg.getModel().createResource();
        cfg.addProperty( prop, jmx );
        return jmx;
    }

	private static Resource getCred(Resource cfg) {
	    return getProperty( cfg, VocabCassandra.credentials );
	}

	private static Resource getJmx(Resource cfg) {
	    return  getProperty( cfg, VocabCassandra.jmx);
	}

	/**
	 * Execute a load from a number of URLs.
	 *
	 * @param connection
	 *            The Cassandra connection to use.
	 * @param keyspace
	 *            The keyspace to load the URLs into.
	 * @param urls
	 *            The urls to load.
	 */
	public static void execute(CassandraConnection connection, final String keyspace, List<String> urls) {
		ExecutorService executor = Executors.newFixedThreadPool(Math.min(4, urls.size()));

		for (String uri : urls) {
			executor.execute(new Runnable() {

				@Override
				public void run() {
					StreamRDFCassandra sink = new StreamRDFCassandra(connection, keyspace);
					RDFDataMgr.parse(sink, uri);
				}
			});
		}

		executor.shutdown();
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
}
