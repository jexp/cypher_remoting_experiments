package de.jexp.zmq;

import net.asdfa.msgpack.InvalidMsgPackDataException;
import net.asdfa.msgpack.MsgPack;
import org.zeromq.ZMQ;

public class hwclient {
    public static void main(String[] args) {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket socket = context.socket(ZMQ.REQ);

        System.out.println("Connecting to hello world server...");
        socket.connect("tcp://localhost:5555");

        //int request_nbr = 0;
        for(int request_nbr = 0; request_nbr != 10; request_nbr++) {
        String query = "start n=node(0) match p=n-[r:KNOWS]->m return p,n,r,m,nodes(p) as nodes, rels(p) as rels,length(p) as length";
        byte[] request = MsgPack.pack(query);
        System.out.println("Sending request " + request_nbr + "...");
        socket.send(request, 0);

        boolean more = false;
        try {
            do {
                byte[] reply = socket.recv(0);
                more = socket.hasReceiveMore();
                // System.out.println(" length " + reply.length + " more " + more);
                if (!more)
                 System.out.println("Received reply " + request_nbr + ": [" + MsgPack.unpack(reply, MsgPack.UNPACK_RAW_AS_STRING) + "]");
            }  while (more) ;
            } catch(Exception e){
                System.err.println("Error unpacking ");
                e.printStackTrace();
            }
            }
        }
    }