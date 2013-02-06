package de.jexp.zmq;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * @author mh
 * @since 06.02.13
 */
public class CypherServerKernelExtensionFactory extends KernelExtensionFactory<CypherServerKernelExtensionFactory.Dependencies> {

    public CypherServerKernelExtensionFactory() {
        super(CypherServer.SERVICE_NAME);
    }

    @Override
    public Lifecycle newKernelExtension(Dependencies dependencies) throws Throwable {
        return new CypherServer(dependencies.getGraphDatabaseService(),dependencies.getStringLogger());
    }

    public interface Dependencies {
        GraphDatabaseService getGraphDatabaseService();
        StringLogger getStringLogger();
    }
}
