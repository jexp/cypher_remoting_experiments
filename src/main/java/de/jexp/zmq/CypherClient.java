package de.jexp.zmq;

import net.asdfa.msgpack.MsgPack;
import org.zeromq.ZMQ;

import java.util.*;
import java.util.concurrent.*;

import static de.jexp.zmq.CypherServer.*;
import static org.neo4j.helpers.collection.MapUtil.map;

/*
MAVEN_OPTS="-Djava.library.path=/usr/local/lib" mvn exec:java -Dexec.mainClass=de.jexp.zmq.CypherClient -Dexec.arg="create n={name:'foo'}"
 */

public class CypherClient {

    private static final String ADDRESS = "tcp://localhost:5555";
    private static final int THREADS = 1;
    private final ThreadLocal<ZMQ.Socket> sockets = new ThreadLocal<ZMQ.Socket>() {
        protected ZMQ.Socket initialValue() {
            ZMQ.Socket socket = context.socket(ZMQ.REQ);
            System.out.println("Connecting to cypher server...");
            socket.connect(ADDRESS);
            return socket;
        }
    };

    private final ZMQ.Context context;

    public CypherClient() {
        context = ZMQ.context(1);
    }

    private ZMQ.Socket socket() {
        return sockets.get();
    }
    public int query(String query, Map params, Map opts, final List<Object> res) {
        final Map<String,Object> requestData = map(QUERY, query, STATS, false, PARAMS, params);
        requestData.putAll(opts);
        byte[] request = MsgPack.pack(requestData);
        // System.out.println("Sending request " + round + "...");
        final ZMQ.Socket socket = socket();
        socket.send(request, 0);

        boolean more;
        int bytes = 0;
        try {
            do {
                byte[] reply = socket.recv(0);
                more = socket.hasReceiveMore();
                bytes += reply.length;
                // System.out.println(" length " + reply.length + " more " + more);
                if (res!=null) {
                    final Object row = MsgPack.unpack(reply, MsgPack.UNPACK_RAW_AS_STRING);
                    // System.out.println("Received reply " + ": [" + row + "]");
                    // if (!more) System.out.println("Received footer " + ": [" + res + "]");
                    res.add(row);
                }
            } while (more);
        } catch (Exception e) {
            System.err.println("Error unpacking ");
            e.printStackTrace();
        }
        return bytes;
    }
    
    void close() {
        // todo correctly
        socket().disconnect(ADDRESS);
        //context.term();
    }
    private static final int ROUNDS = 1;

    public static void main(String[] args) throws Exception {
        final CypherClient client = new CypherClient();
        long time=System.currentTimeMillis();
        String query = args.length>0 ? args[0] : "create n={name:{name}}";
        // query = "start n=node(0) match p=n-[r:KNOWS]->m return p,n,r,m,nodes(p) as nodes, rels(p) as rels,length(p) as length";
        // for (int i=0;i<10;i++) testCreate(client);
        final int bytes = testMulti(client, ROUNDS, THREADS);
        System.out.println(ROUNDS*10+" queries took "+(System.currentTimeMillis()-time)+" ms for "+bytes+" bytes.");
        client.close();
    }

    private static int testMulti(final CypherClient client, final int rounds, final int threads) throws Exception {
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        final String query = "create n={name:{name}}";
        Collection<Future<Integer>> futures=new ArrayList<Future<Integer>>(rounds);
        for (int round = 0; round < rounds *10; round++) {
            final int finalRound = round;
            final Callable<Integer> task = new Callable<Integer>() {
                public Integer call() {
                    return client.query(query, Collections.singletonMap("name", "test" + finalRound), map(NO_RESULTS, true), null);
                }
            };
            futures.add(pool.submit(task));
        }
        int result=0;
        for (Future<Integer> future : futures) {
            result+=future.get(50,TimeUnit.SECONDS);
        }
        pool.shutdown();
        pool.awaitTermination(60, TimeUnit.SECONDS);
        return result;
    }
    private static void testCreate(CypherClient client) {
        String query = "create n={name:{name}}";
        // System.out.println("query "+query);
        final ArrayList<Object> res = new ArrayList<Object>();
        client.query(null, null, map(TX, "begin",STATS,true), res);
        Number txId = (Number) ((Map)res.get(res.size()-1)).get("tx_id");
        for (int round = 0; round < ROUNDS; round++) {
            client.query(query, Collections.singletonMap("name", "test" + round),map(TX_ID,txId, NO_RESULTS,false),null);
        }
        client.query(null, null, map(TX, "commit",TX_ID,txId), null);
    }
}