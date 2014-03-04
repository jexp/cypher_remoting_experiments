package de.jexp.msgpack;

import net.asdfa.msgpack.MsgPack;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Collections;
import java.util.Map;

/**
 * @author mh
 * @since 20.01.13
 */
public class CypherResultMessagePackTest {

    private final GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
    private final ExecutionEngine executionEngine = new ExecutionEngine(db);
    private Transaction tx;

    @Before
    public void setUp() throws Exception {
        tx = db.beginTx();
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
    }

    @After
    public void tearDown() throws Exception {
        if (tx!=null) {
            tx.failure();
        }
    }

    @Test
    public void testPackCypherResult() throws Exception {
        ExecutionResult result = executionEngine.execute("start n=node(0) match p=n-[r:KNOWS]->m return p,n,r,m,nodes(p) as nodes, rels(p) as rels,length(p) as length");
        for (Map<String, Object> row : result) { }
        result = executionEngine.execute("start n=node(0) match p=n-[r:KNOWS]->m return p,n,r,m,nodes(p) as nodes, rels(p) as rels,length(p) as length");
        final ExecutionResultMessagePack packedResult = new ExecutionResultMessagePack(result);
        int count=0;
        int row=0;
        while (packedResult.hasNext()) {
            byte[] next = packedResult.next();
            count += next.length;
            if (row < 2 || !packedResult.hasNext())
                System.out.println(MsgPack.unpack(next,MsgPack.UNPACK_RAW_AS_STRING)+ " bytes "+count);
            row++;
        }
    }
}
