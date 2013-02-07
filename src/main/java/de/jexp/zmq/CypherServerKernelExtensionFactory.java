package de.jexp.zmq;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static org.neo4j.helpers.Settings.*;
import static org.neo4j.helpers.Settings.ANY;

/**
 * @author mh
 * @since 06.02.13
 */
public class CypherServerKernelExtensionFactory extends KernelExtensionFactory<CypherServerKernelExtensionFactory.Dependencies> {

    @Description("Settings for the Cypher Remoting Extension")
    public static abstract class CypherRemotingSettings {
        public static GraphDatabaseSetting.HostnamePortSetting cypher_remoting_address = new GraphDatabaseSetting.HostnamePortSetting(setting("cypher_remoting_address", HOSTNAME_PORT, ":5555" )); // todo illegalmessage
        public static GraphDatabaseSetting.IntegerSetting cypher_remoting_threads = new GraphDatabaseSetting.IntegerSetting(setting("cypher_remoting_threads", INTEGER, "1", illegalValueMessage("must be a thread number between 1 and 10",range(1,10))));
    }

    public CypherServerKernelExtensionFactory() {
        super(CypherServer.SERVICE_NAME);
    }

    @Override
    public Lifecycle newKernelExtension(Dependencies dependencies) throws Throwable {
        Config config = dependencies.getConfig();
        return new CypherServer(dependencies.getGraphDatabaseService(),dependencies.getStringLogger(), config.get(CypherRemotingSettings.cypher_remoting_address),config.get(CypherRemotingSettings.cypher_remoting_threads));
    }

    public interface Dependencies {
        GraphDatabaseService getGraphDatabaseService();
        StringLogger getStringLogger();
        Config getConfig();
    }
}
