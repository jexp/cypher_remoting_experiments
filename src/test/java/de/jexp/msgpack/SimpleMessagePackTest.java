package de.jexp.msgpack;

import org.junit.Test;
import net.asdfa.msgpack.MsgPack;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 20.01.13
 */
public class SimpleMessagePackTest {

    private final List<?> objects = Arrays.asList("potatoes", false, 0, 11, null,Collections.singletonMap("foo","bar"));

    @Test
    public void testPackData() throws Exception {
        //pack it:
        byte[] data = MsgPack.pack(objects);
        //unpack it:
        Object unpacked = MsgPack.unpack(data,MsgPack.UNPACK_RAW_AS_STRING);
        System.out.println("Unpacked data: " + unpacked);
        assertEquals(objects,unpacked);

        final FileOutputStream fos = new FileOutputStream("packed.msgpack");
        fos.write(data);
        fos.close();
    }
    @Test
    public void testPackDataWithTrailingZero() throws Exception {
        //pack it:
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(bos);
        MsgPack.pack(objects, out);
        out.write(0);
        //unpack it:
        System.out.println(new String(bos.toByteArray()));
        System.out.println(Arrays.toString(bos.toByteArray()));
        Object unpacked = MsgPack.unpack(bos.toByteArray(),MsgPack.UNPACK_RAW_AS_STRING);
        System.out.println("Unpacked data: " + unpacked);
        assertEquals(objects, unpacked);
    }

    /*
    read from ruby

    yrintri:zeromq_messagepack mh$ irb
    >> require 'msgpack'
    => true
    >> contents = open("packed.msgpack", "rb") {|io| io.read }
    => "\225\250potatoes\302\v\300\201\243foo\243bar"
    >> MessagePack.unpack(contents)
    => ["potatoes", false, 11, nil, {"foo"=>"bar"}]

    */

    @Test
    public void testPackToStream() throws Exception {
        for (int i=0;i<10000;i++) {
            MsgPack.pack(objects);
        }
        long time = System.currentTimeMillis();
        long counter=0;
        for (int i=0;i<1;i++) {
            counter+=MsgPack.pack(objects).length;
        }
        System.out.println("took "+(System.currentTimeMillis()-time)+" ms for "+counter+" bytes.");
    }
    /*
      Node : { id : id, [data : {foo:bar}]}
      Relationship : { id : id, start: id, end: id, type : "FOO",  [data : {foo:bar}]}
      Path {start: node, nodes: [nodes], relationships [], end: node, lenght: 1}
    */
    interface Node {
        long getId();
        Iterable<String> getPropertyKeys();
        Object getProperty(String name);
    }
}
