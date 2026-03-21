package protocols.membership.hyparview.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class ForwardJoinMessage extends ProtoMessage {

    public static final short MSG_ID = 602;

    private final Host newNode;
    private final int timeToLive;
    private final Host sender;

    public ForwardJoinMessage(Host newNode, int timeToLive, Host sender) {
        super(MSG_ID);
        this.newNode = newNode;
        this.timeToLive = timeToLive;
        this.sender = sender;
    }

    public Host getNewNode() {
        return newNode;
    }

    public int getTimeToLive() {
        return timeToLive;
    }

    public Host getSender() {
        return sender;
    }

    @Override
    public String toString() {
        return "ForwardJoinMessage{newNode=" + newNode + ", ttl=" + timeToLive + ", sender=" + sender + "}";
    }

    public static ISerializer<ForwardJoinMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ForwardJoinMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.newNode, out);
            out.writeInt(msg.timeToLive);
            Host.serializer.serialize(msg.sender, out);
        }

        @Override
        public ForwardJoinMessage deserialize(ByteBuf in) throws IOException {
            Host newNode = Host.serializer.deserialize(in);
            int ttl = in.readInt();
            Host sender = Host.serializer.deserialize(in);
            return new ForwardJoinMessage(newNode, ttl, sender);
        }
    };
}