package de.jexp.zmq;

import de.jexp.msgpack.ExecutionResultMessagePack;
import de.jexp.transaction.TransactionRegistry;
import net.asdfa.msgpack.MsgPack;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import org.zeromq.ZMQException;
import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.neo4j.helpers.collection.MapUtil.map;

public class CypherServer implements Lifecycle {
    public static final String SERVICE_NAME = "CYPHER_REMOTING";
    
    private static final String WORKER_ADDRESS = "inproc://workers";

    private final int numThreads;

    public final static String TX_ID = "tx_id";
    public final static String TX = "tx";
    public final static String PARAMS = "params";
    public final static String QUERY = "query";
    public final static String STATS = "stats";
    public final static String NO_RESULTS = "no_results";
    private static final byte[] EMPTY_MSG = MsgPack.pack(Collections.EMPTY_MAP);

    private final StringLogger logger;
    private final ExecutionEngine engine;
    private final TransactionRegistry transactionRegistry;
    
    private ZContext context = new ZContext();;

    private AtomicBoolean running=new AtomicBoolean(false);
    private final List<CypherExecutor> executors;

    private final String externalAddress;

    public CypherServer(GraphDatabaseService db, StringLogger logger, HostnamePort hostnamePort, Integer numThreads) {
        this.logger = logger;
        engine = new ExecutionEngine(db);
        transactionRegistry = new TransactionRegistry(db);
        externalAddress = "tcp://" + hostnamePort.getHost("*")+":"+hostnamePort.getPort();
        this.numThreads=numThreads;
        executors = new ArrayList<CypherExecutor>(numThreads);
    }



    public static void main(final String[] args) throws Throwable {
        final File directory = new File(args[0]);
        boolean newDB=!directory.exists();
        System.out.println("Using database "+directory+" new "+newDB);
        final GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( args[0] );
        if (newDB) initialize(db);
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                db.shutdown();
            }
        });
        mainThread.join();
    }

    private static Map<String,Object> beforeQuery(TransactionRegistry transactionRegistry, String tx, Number txId) throws Exception {
        if ("begin".equals(tx)) {
            return map(TX_ID,transactionRegistry.createTransaction(), TX,"begin");
        }
        if (txId != null) {
            transactionRegistry.selectCurrentTransaction(txId.longValue());
            return map(TX_ID,txId);
        }
        if ("rollback".equals(tx)) {
            transactionRegistry.rollbackCurrentTransaction();
            return map(TX_ID,-1, TX,"rollback");
        }
        return Collections.emptyMap();
    }

    private static Map<String,Object> afterQuery(TransactionRegistry transactionRegistry, String tx) throws Exception {
        if ("commit".equals(tx)) {
            transactionRegistry.commitCurrentTransaction();
            return map(TX_ID, -1, TX, "commit");
        } else {
            transactionRegistry.suspendCurrentTransaction();
            return Collections.emptyMap();
        }
    }

    private static void initialize(GraphDatabaseService db) {
        Transaction tx = db.beginTx();
        final Node refNode = db.createNode();
        refNode.setProperty("name", "Name");
        refNode.setProperty("age", 42);
        refNode.setProperty("married", true);
        refNode.setProperty("kids", new String[]{"foo", "bar"});
        refNode.setProperty("bytes", new byte[]{(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef});

        for (int i=0;i<1000;i++) {
            final Relationship rel = refNode.createRelationshipTo(db.createNode(), DynamicRelationshipType.withName("KNOWS"));
            rel.setProperty("since",1900L);
            rel.setProperty("weight",42D);
        }
        tx.success();
    }


    @Override
    public void init() throws Throwable {

    }

    @Override
    public void start() throws Throwable {
        
        running.set(true);

        if (numThreads >1) {
            
            final ZMQ.Socket router = context.createSocket((ZMQ.ROUTER));
            router.bind(externalAddress);

            final ZMQ.Socket workers = context.createSocket(ZMQ.DEALER);
            workers.bind(WORKER_ADDRESS);

            for (int thread=0;thread< numThreads;thread++){
                executors.add(new CypherExecutor(WORKER_ADDRESS,true));
            }

            startDaemonThread(new Runnable() {
                public void run() {
                    ZMQ.proxy(router, workers, null);
                }
            });
        } else {
            executors.add(new CypherExecutor(externalAddress,false));
        }
        
        // socket.setReceiveTimeOut(ms);
        // socket.setSendTimeOut(ms);
        // socket.setLinger(ms);
        // high-water-mark, socket.setHWM(), socket.setSwap
        // subscribe (byte [] topic), e.g. to differentiate between read and write queries or cypher and non-cypher


        for (CypherExecutor executor : executors) {
            startDaemonThread(executor);
        }
        logger.info("Started Cypher Remoting on external address " + externalAddress + " with " + numThreads+" threads");
    }

    private void startDaemonThread(Runnable runnable) {
        final Thread thread = new Thread(runnable);
        //thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void stop() throws Throwable {
        running.set(false);
        for (CypherExecutor executor : executors) {
            executor.stop();
        }
        context.destroy();
    }

    @Override
    public void shutdown() throws Throwable {
        stop();
    }

    class CypherExecutor implements Runnable {
        private ZMQ.Socket socket;

        CypherExecutor(String address, boolean connect) {
            socket = context.createSocket(ZMQ.REP);
            socket.setTCPKeepAlive(1);
            if (connect) socket.connect(address); 
            else socket.bind(address);
        }

        @Override
        public void run() {

            while (!Thread.currentThread().isInterrupted() && running.get()) {

                try {
                    byte[] request = socket.recv (0);
                    final Object data = MsgPack.unpack(request, MsgPack.UNPACK_RAW_AS_STRING);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Cypher Remoting, got query " + data);
                    }
                    boolean stats = false;
                    ExecutionResult result = null;
                    Map<String, Object> info = new HashMap<String, Object>();
                    if (data instanceof String) {
                        result = execute(Collections.singletonMap(QUERY, (String) data), info);
                    }
                    if (data instanceof Map) {
                        result = execute((Map) data, info);
                    }
                    final ExecutionResultMessagePack messagePack = new ExecutionResultMessagePack(result, stats, info);
                    if (!messagePack.hasNext()) {
                        socket.send(EMPTY_MSG, 0);
                    } else {
                        while (messagePack.hasNext()) {
                            byte[] next = messagePack.next();
                            socket.send(next, messagePack.hasNext() ? ZMQ.SNDMORE : 0);
                        }
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("Cypher Remoting, result stats " + messagePack.createResultInfo());
                    }

                } catch (ZMQException e) {
                    if (e.getErrorCode () == ZMQ.Error.ETERM.getCode ()) {
                        break;
                    }
                } catch (Exception e) {
                    logger.warn("Error during remote cypher execution ", e);
                    final Map<String, Object> result = map();
                    ExecutionResultMessagePack.addException(result, e);
                    socket.send(MsgPack.pack(result), 0);
                }

            }
        }

        public void stop() {
            if (!running.get()) {
                context.destroySocket(socket);
            }
        }
    }

    private ExecutionResult execute(Map input, Map<String, Object> info) throws Exception {
        boolean stats = Boolean.TRUE.equals(input.get(STATS));
        final Number txId = (Number) input.get(TX_ID);
        final String tx = (String) input.get(TX);

        Map<String,Object> params = input.containsKey(PARAMS) ? (Map<String,Object>) input.get(PARAMS) : Collections.<String,Object>emptyMap();
        final String query = (String) input.get(QUERY);

        info.putAll(beforeQuery(transactionRegistry, tx, txId));

        ExecutionResult result = null;
        if (query != null) result = engine.execute(query,params);

        if (input.containsKey(NO_RESULTS)) result=null;

        info.putAll(afterQuery(transactionRegistry, tx));
        return result;
    }
}