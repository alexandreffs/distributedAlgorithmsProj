package protocols.membership.hyparview.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class ForwardJoinMessage extends ProtoMessage {

    public static final short MSG_ID = 602;

    private final Host newNode;
    private final int ttl;

    public ForwardJoinMessage(Host newNode, int ttl) {
        super(MSG_ID);
        this.newNode = newNode;
        this.ttl = ttl;
    }

    public Host getNewNode() {
        return newNode;
    }

    public int getTimeToLive() {
        return ttl;
    }

    @Override
    public String toString() {
        return "ForwardJoinMessage{" +
                "newNode=" + newNode +
                ", ttl=" + ttl +
                '}';
    }

    public static final ISerializer<ForwardJoinMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ForwardJoinMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.newNode, out);
            out.writeInt(msg.ttl);
        }

        @Override
        public ForwardJoinMessage deserialize(ByteBuf in) throws IOException {
            Host newNode = Host.serializer.deserialize(in);
            int ttl = in.readInt();
            return new ForwardJoinMessage(newNode, ttl);
        }
    };
}