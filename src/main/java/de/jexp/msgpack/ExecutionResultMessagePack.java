package de.jexp.msgpack;

import net.asdfa.msgpack.MsgPack;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.cypher.javacompat.QueryStatistics;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.MapUtil;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.util.*;

import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author mh
 * @since 20.01.13
 */
public class ExecutionResultMessagePack implements Iterator<byte[]> {
    private static final int FIRST = Integer.MIN_VALUE;
    private static final int LAST = Integer.MAX_VALUE;
    private final ExecutionResult result;
    private final boolean stats;
    int row = FIRST;
    private Iterator<Map<String, Object>> it;
    private List<String> columns = null;
    private final List<Object> data;
    private final long start;
    private Exception exception;
    private long bytes=0;

    public ExecutionResultMessagePack(ExecutionResult result, boolean stats) {
        this.result = result;
        this.stats = stats;
        try {
            columns = this.result.columns();
            it = this.result.iterator();
        } catch (Exception e) {
            this.exception = e;
            if (columns==null) {
                columns = Collections.emptyList();
            }
            if (it==null) {
                it = emptyIterator();
            }
        }
        data = new ArrayList<Object>(columns);
        start = System.currentTimeMillis();
    }

    private Iterator<Map<String, Object>> emptyIterator() {
        return Collections.<Map<String, Object>>emptyList().iterator();
    }

    public boolean hasNext() {
        return row == FIRST || it.hasNext() || row != LAST || exception!=null;
    }

    public byte[] next() {
        if (row == FIRST) {
            row = 0;
            return pack(columns);
        }
        if (it.hasNext()) {
            try {
                final Map<String, Object> current = it.next();
                for (int i = 0; i < columns.size(); i++) {
                    final Object value = current.get(columns.get(i));
                    data.set(i, convert(value));
                }
                row++;
                return pack(data);
            } catch(Exception e) {
                exception = e;
                it = emptyIterator();
                return pack(info());
            }
        } else {
            return pack(info());
        }
    }

    private byte[] pack(Object data) {
        final byte[] result = MsgPack.pack(data);
        bytes += result.length;
        return result;
    }

    private Map<String,Object> toMap(PropertyContainer pc) {
        final Iterator<String> propertyKeys = pc.getPropertyKeys().iterator();
        if (!propertyKeys.hasNext()) return null;
        Map<String,Object> result = new LinkedHashMap<String,Object>();
        while (propertyKeys.hasNext()) {
            String prop = propertyKeys.next();
            final Object value = pc.getProperty(prop);
            if (value.getClass().isArray()) {
                final int length = Array.getLength(value);
                final ArrayList<Object> list = new ArrayList<Object>(length);
                for (int i=0;i< length;i++) {
                    list.add(Array.get(value, i));
                }
                result.put(prop, list);
            } else {
                result.put(prop, value);
            }
        }
        return result;
    }
    private Object convert(Object value) {
        if (value == null) return null;
        if (value instanceof Node) {
            final Node node = (Node) value;
            return returnWithProps(map("id", node.getId()), node);
        }
        if (value instanceof Relationship) {
            Relationship relationship = (Relationship) value;
            return returnWithProps(map("id", relationship.getId(), "start", relationship.getStartNode().getId(), "end", relationship.getEndNode().getId(), "type", relationship.getType().name()), relationship);
        }
        if (value instanceof Path) {
            Path path = (Path) value;
            return map(
                    "length", path.length(),
                    "start", convert(path.startNode()),
                    "end", convert(path.endNode()),
                    "nodes", convert(path.nodes()),
                    "relationships", convert(path.relationships()));
        }
        if (value instanceof Iterable) {
            return convert(((Iterable) value).iterator());
        }
        if (value instanceof Iterator) {
            final ArrayList<Object> result = new ArrayList<Object>();
            Iterator iterator = (Iterator) value;
            while (iterator.hasNext()) {
                result.add(convert(iterator.next()));
            }
            return result;
        }
        // todo support primitive arrays directly
        if (value.getClass().isArray()) {
            final ArrayList<Object> result = new ArrayList<Object>();
            final int length = Array.getLength(value);
            for (int i=0;i< length;i++) {
                result.add(convert(Array.get(value,i)));
            }
            return result;
        }
        return value;
    }

    private Object returnWithProps(Map<String, Object> result, PropertyContainer pc) {
        final Map<String, Object> props = toMap(pc);
        if (props!=null) {
            result.put("data",props);
        }
        return result;
    }

    private Map<String, Object> info() {
        if (!stats && exception==null) {
            row = LAST;
            return Collections.EMPTY_MAP;
        }
        final QueryStatistics stats = result.getQueryStatistics();
        final Map<String, Object> result = MapUtil.map(
                "time", System.currentTimeMillis() - start,
                "rows", row,
                "bytes", bytes,
                "updates", stats.containsUpdates());
        row = LAST;
        if (stats.containsUpdates()) {
            putIfValue(result, "nodes_deleted", stats.getDeletedNodes());
            putIfValue(result, "nodes_created", stats.getNodesCreated());
            putIfValue(result, "rels_created", stats.getRelationshipsCreated());
            putIfValue(result, "rels_deleted", stats.getDeletedRelationships());
            putIfValue(result, "props_set", stats.getPropertiesSet());
        }
        if (exception!=null) {
            // TODO log
            exception.printStackTrace();
            result.put("error",exception.getMessage());

            final StringWriter writer = new StringWriter();
            exception.printStackTrace(new PrintWriter(writer));
            result.put("exception", writer.toString());
            exception = null;
        }
        return result;
    }

    private void putIfValue(Map<String, Object> result, String name, int value) {
        if (value >0) {
            result.put(name, value);
        }
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}
