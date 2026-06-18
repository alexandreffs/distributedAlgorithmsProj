package protocols.membership.hyparview.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class JoinMessage extends ProtoMessage {

    public static final short MSG_ID = 601;

    private final Host newNode;

    public JoinMessage(Host newNode) {
        super(MSG_ID);
        this.newNode = newNode;
    }

    public Host getNewNode() {
        return newNode;
    }

    @Override
    public String toString() {
        return "JoinMessage{" +
                "newNode=" + newNode +
                '}';
    }

    public static final ISerializer<JoinMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.newNode, out);
        }

        @Override
        public JoinMessage deserialize(ByteBuf in) throws IOException {
            Host newNode = Host.serializer.deserialize(in);
            return new JoinMessage(newNode);
        }
    };
}