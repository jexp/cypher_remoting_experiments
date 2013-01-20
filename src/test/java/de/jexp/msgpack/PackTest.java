package de.jexp.msgpack;

import static org.junit.Assert.assertEquals;

import net.asdfa.msgpack.MsgPack;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.annotation.Message;
import org.msgpack.packer.BufferPacker;
import org.msgpack.packer.Unconverter;
import org.msgpack.type.Value;
import org.msgpack.unpacker.BufferUnpacker;
import org.msgpack.unpacker.Converter;

import java.util.Arrays;
import java.util.List;

public class PackTest {
    @Message
        public static class TestMessage {
            public boolean[] b = new boolean[0];
            public short[] s = new short[0];
            public int[] i = new int[0];
            // public long[] l = new long[0]; // FIXME javassist?
            public float[] f = new float[0];

            // public double[] d = new double[0]; // FIXME javassist?

            public TestMessage() {
            }
        }

    @Test
    public void testTime() throws Exception {
        List<?> objects = Arrays.asList("potatoes", false, 11, null);        MessagePack msgpack = new MessagePack();
        long time = System.currentTimeMillis();
        long counter=0;
        for (int i=0;i<10000;i++) {
            BufferPacker packer = msgpack.createBufferPacker();
            for (int j=0;j<100;j++) {
                packer.write(objects);
            }
            counter+= packer.toByteArray().length;
        }
        System.out.println("took " + (System.currentTimeMillis() - time) + " ms for " + counter + " bytes.");

    }

    @Test
    public void testPrimitive() throws Exception {
        MessagePack msgpack = new MessagePack();
        BufferPacker packer = msgpack.createBufferPacker();

        TestMessage t = new TestMessage();
        t.b = new boolean[] { true, false };
        t.s = new short[] { 0, 1 };
        t.i = new int[] { 2, 3 };
        // t.l = new long[] {4, 5};
        t.f = new float[] { 2.0f, 4.0f };
        // t.d = new double[] {8.0, 16.0};

        packer.write(t);
        byte[] raw = packer.toByteArray();
        BufferUnpacker unpacker = msgpack.createBufferUnpacker(raw);
        TestMessage u = unpacker.read(TestMessage.class);
        assertEquals(t.b.length, u.b.length);
        for (int i = 0; i < t.b.length; i++) {
            assertEquals(t.b[i], u.b[i]);
        }
        assertEquals(t.s.length, u.s.length);
        for (int i = 0; i < t.s.length; i++) {
            assertEquals(t.s[i], u.s[i]);
        }
        assertEquals(t.i.length, u.i.length);
        for (int i = 0; i < t.i.length; i++) {
            assertEquals(t.i[i], u.i[i]);
        }
        // assertEquals(t.l.length, u.l.length);
        // for(int i=0; i < t.l.length; i++) { assertEquals(t.l[i], u.l[i]); }
        assertEquals(t.f.length, u.f.length);
        for (int i = 0; i < t.f.length; i++) {
            assertEquals(t.f[i], u.f[i], 10e-10);
        }
        // assertEquals(t.d.length, u.d.length);
        // for(int i=0; i < t.d.length; i++) { assertEquals(t.d[i], u.d[i]); }

        Unconverter unconverter = new Unconverter(msgpack);
        unconverter.write(t);
        Value value = unconverter.getResult();
        Converter converter = new Converter(msgpack, value);
        TestMessage c = converter.read(TestMessage.class);
        assertEquals(t.b.length, c.b.length);
        for (int i = 0; i < t.b.length; i++) {
            assertEquals(t.b[i], c.b[i]);
        }
        assertEquals(t.s.length, c.s.length);
        for (int i = 0; i < t.s.length; i++) {
            assertEquals(t.s[i], c.s[i]);
        }
        assertEquals(t.i.length, c.i.length);
        for (int i = 0; i < t.i.length; i++) {
            assertEquals(t.i[i], c.i[i]);
        }
        // assertEquals(t.l.length, c.l.length);
        // for(int i=0; i < t.l.length; i++) { assertEquals(t.l[i], c.l[i]); }
        assertEquals(t.f.length, c.f.length);
        for (int i = 0; i < t.f.length; i++) {
            assertEquals(t.f[i], c.f[i], 10e-10);
        }
        // assertEquals(t.d.length, c.d.length);
        // for(int i=0; i < t.d.length; i++) { assertEquals(t.d[i], c.d[i]); }
    }
}
