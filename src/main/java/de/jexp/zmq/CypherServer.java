package de.jexp.zmq;

import de.jexp.msgpack.ExecutionResultMessagePack;
import net.asdfa.msgpack.InvalidMsgPackDataException;
import net.asdfa.msgpack.MsgPack;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.zeromq.ZMQ;

import java.io.File;
import java.util.Collections;
import java.util.Map;
/*
MAVEN_OPTS="-Djava.library.path=/usr/local/lib -Xmx256M -Xms256M -server -d64" mvn exec:java -Dexec.mainClass=de.jexp.zmq.CypherServer -Dexec.args=graph.db
 */

public class CypherServer {
    public static void main(String[] args) {

        final File directory = new File(args[0]);
        boolean newDB=!directory.exists();
        System.out.println("Using database "+directory+" new "+newDB);
        final EmbeddedGraphDatabase db = new EmbeddedGraphDatabase(args[0]);
        final ExecutionEngine engine = new ExecutionEngine(db);
        if (newDB) initialize(db);
        Runtime.getRuntime().addShutdownHook(new Thread(){ @Override public void run() { db.shutdown(); } });

        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket socket = context.socket(ZMQ.REP);
        socket.bind ("tcp://*:5555");

        while (true) {
            byte[] request = socket.recv(0);
            try {
                final Object data = MsgPack.unpack(request, MsgPack.UNPACK_RAW_AS_STRING);
                // System.out.println(data);
                boolean stats=false;
                ExecutionResult result = null;
                if (data instanceof String) {
                    result = engine.execute((String) data, Collections.EMPTY_MAP);
                }
                if (data instanceof Map) {
                    final Map input = (Map) data;
                    stats = Boolean.TRUE.equals(input.get("stats"));
                    result = engine.execute((String) input.get("query"),(Map) input.get("params"));
                }
                if (result!=null) {
                    final ExecutionResultMessagePack messagePack = new ExecutionResultMessagePack(result,stats);
                    while (messagePack.hasNext()) {
                        byte[] next = messagePack.next();
                        socket.send(next,messagePack.hasNext() ? ZMQ.SNDMORE : 0);
                    }
                }
            } catch (InvalidMsgPackDataException e) {
                e.printStackTrace();
            }
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