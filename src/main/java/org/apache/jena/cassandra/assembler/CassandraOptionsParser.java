package org.apache.jena.cassandra.assembler;

import static org.apache.jena.sparql.util.graph.GraphUtils.atmostOneProperty;
import static org.apache.jena.sparql.util.graph.GraphUtils.exactlyOneProperty;
import static org.apache.jena.sparql.util.graph.GraphUtils.getResourceValue;
import static org.apache.jena.sparql.util.graph.GraphUtils.getStringValue;
import static org.apache.jena.sparql.util.graph.GraphUtils.multiValueString;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import org.apache.jena.assembler.exceptions.AssemblerException;
import org.apache.jena.rdf.model.Resource;

public class CassandraOptionsParser {

    private CassandraOptionsParser() {
    }

    public static String parseName(Resource root) {
        if (!exactlyOneProperty(root, VocabCassandra.name)) {
            throw new AssemblerException(root,
                    String.format("%s must be specified", VocabCassandra.name.getLocalName()));
        }
        return getStringValue(root, VocabCassandra.name);
    }

    public static boolean parseSSL(Resource root) {
        String ssl = getStringValue(root, VocabCassandra.ssl);
        return ssl != null && Boolean.valueOf(ssl);
    }

    public static void parseAddress(Resource root, Consumer<String> consumer ) {
        multiValueString(root, VocabCassandra.address).forEach( consumer );
    }

    public static void parsePort(Resource root, IntConsumer consumer ) {
        String port = getStringValue(root, VocabCassandra.port);
        if (port != null) {
            try {
                consumer.accept( Integer.valueOf(port) );
            } catch (NumberFormatException e) {
                throw new AssemblerException(root, String.format("Port (%s) must be a number", port));
            }
        }
    }

    public static void parseCredentials( Resource root, BiConsumer<String,String> consumer ) {
        if (!atmostOneProperty(root, VocabCassandra.credentials)) {
            throw new AssemblerException(root,
                    String.format("At most one %s may be specified", VocabCassandra.credentials.getLocalName()));
        }

        Resource credentials = getResourceValue(root, VocabCassandra.credentials);
        if (credentials != null) {
            String username = getStringValue(credentials, VocabCassandra.user);
            if (username == null) {
                throw new AssemblerException(root, String.format("If %s is specified %s must be specified",
                        VocabCassandra.credentials.getLocalName(), VocabCassandra.user.getLocalName()));
            }
            String password = getStringValue(credentials, VocabCassandra.password);
            if (password == null) {
                throw new AssemblerException(root, String.format("If %s is specified %s must be specified",
                        VocabCassandra.credentials.getLocalName(), VocabCassandra.password.getLocalName()));
            }
            consumer.accept(username, password);
        }
    }

    public static int parseThreadCount( Resource root, int dflt ) {
        if (!atmostOneProperty(root, VocabCassandra.threadCount)) {
            throw new AssemblerException(root,
                    String.format("%s may be specified only once.", VocabCassandra.threadCount.getLocalName()));
        }
        String value = getStringValue(root, VocabCassandra.threadCount);
        if (value==null) {
            return dflt;
        }
        try {
            return Integer.parseInt( value );
        } catch (NumberFormatException e) {
            throw new AssemblerException(root,
                    String.format("%s is not a valid integer.", VocabCassandra.threadCount.getLocalName()),e);
        }
    }
}
