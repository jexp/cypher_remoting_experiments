package de.jexp.zmq;

import de.jexp.msgpack.ExecutionResultMessagePack;
import de.jexp.transaction.TransactionRegistry;
import net.asdfa.msgpack.MsgPack;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMQQueue;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.neo4j.helpers.collection.MapUtil.map;
/*
MAVEN_OPTS="-Djava.library.path=/usr/local/lib -Xmx256M -Xms256M -server -d64" mvn exec:java -Dexec.mainClass=de.jexp.zmq.CypherServer -Dexec.args=graph.db
 */

public class ParallelCypherServer {

    public final static String TX_ID = "tx_id";
    public final static String TX = "tx";
    public final static String PARAMS = "params";
    public final static String QUERY = "query";
    public final static String STATS = "stats";
    public final static String NO_RESULTS = "no_results";
    private static final byte[] EMPTY_MSG = MsgPack.pack(Collections.EMPTY_MAP);
    private static final int NUM_THREADS = 5;
    private static final String INPROC_WORKERS = "inproc://workers";

    public static void main(String[] args) throws InterruptedException {

        final File directory = new File(args[0]);
        boolean newDB=!directory.exists();
        System.out.println("Using database "+directory+" new "+newDB);
        final EmbeddedGraphDatabase db = new EmbeddedGraphDatabase(args[0], MapUtil.stringMap("keep_logical_logs","false"));
        final ExecutionEngine engine = new ExecutionEngine(db);
        final TransactionRegistry transactionRegistry = new TransactionRegistry(db);
        if (newDB) initialize(db);

        final ZMQ.Context context = ZMQ.context(1);
        final ZMQ.Socket clients = context.socket(ZMQ.ROUTER);
        clients.bind ("tcp://*:5555");
        System.out.println("clients bound to \"tcp://*:5555\" "+clients);

        final ZMQ.Socket workers = context.socket(ZMQ.DEALER);
        workers.bind(INPROC_WORKERS);
        System.out.println("workers bound to "+INPROC_WORKERS+" "+workers);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                db.shutdown();
                clients.close();
                workers.close();
                context.term();
            }
        });

        for (int i=0;i<NUM_THREADS;i++) {
            final int threadId = i;
            final Thread worker = new Thread() {
                public void run() {
                    ZMQ.Socket socket = context.socket(ZMQ.REP);
                    // socket.setReceiveTimeOut(ms);
                    // socket.setSendTimeOut(ms);
                    // socket.setLinger(ms);
                    // high-water-mark, socket.setHWM(), socket.setSwap
                    // socket.setTCPKeepAlive(1);
                    // subscribe (byte [] topic), e.g. to differentiate between read and write queries or cypher and non-cypher

                    socket.connect(INPROC_WORKERS);
                    System.out.println(threadId+". Connected to worker socket "+socket);
                    while (!Thread.currentThread().isInterrupted()) {
                        byte[] request = socket.recv(0);
                        try {
                            final Object data = MsgPack.unpack(request, MsgPack.UNPACK_RAW_AS_STRING);
                            boolean stats = false;
                            ExecutionResult result = null;
                            if (data instanceof String) {
                                result = engine.execute((String) data, Collections.<String, Object>emptyMap());
                            }
                            Map<String, Object> info = new HashMap<String, Object>();
                            if (data instanceof Map) {
                                final Map input = (Map) data;
                                stats = Boolean.TRUE.equals(input.get(STATS));
                                final Number txId = (Number) input.get(TX_ID);
                                final String tx = (String) input.get(TX);
                                info.putAll(beforeQuery(transactionRegistry, tx, txId));

                                Map<String, Object> params = input.containsKey(PARAMS) ? (Map<String, Object>) input.get(PARAMS) : Collections.<String, Object>emptyMap();
                                final String query = (String) input.get(QUERY);
                                if (query != null) result = engine.execute(query, params);
                                if (input.containsKey(NO_RESULTS)) result = null;
                                info.putAll(afterQuery(transactionRegistry, tx));
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

                        } catch (Exception e) {
                            e.printStackTrace();
                            final Map<String, Object> result = map();
                            ExecutionResultMessagePack.addException(result, e);
                            socket.send(MsgPack.pack(result), 0);
                        }
                    }
                    System.out.println(threadId+". End of run loop "+socket);
                }
            };
            worker.setDaemon(true);
            worker.start();
        }

        final Thread queueThread = new Thread() {
            public void run() {
                ZMQ.proxy(clients,workers,null);
            }
        };
        queueThread.setDaemon(true);
        queueThread.start();
        Thread.currentThread().join();
    }


    private static void tryConnect(ZMQ.Socket socket, String addr) {
        int tries = 10;
        ZMQException lastEx = null;
        while (tries-- > 0) {
            try {
                socket.connect(addr);
                return;
            } catch (ZMQException e) {
                if (!e.getMessage().contains("Connection refused")) throw e;
                lastEx=e;
                System.err.println(socket+" Connection to "+addr+" refused, retry");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    // ignore
                }
            }
        }
        throw lastEx;
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
        final Node refNode = db.getReferenceNode();
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
        tx.finish();
    }
}