package protocols.membership.hyparview.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class DisconnectMessage extends ProtoMessage {

    public static final short MSG_ID = 603;

    private final Host peer;

    public DisconnectMessage(Host peer) {
        super(MSG_ID);
        this.peer = peer;
    }

    public Host getPeer() {
        return peer;
    }

    @Override
    public String toString() {
        return "DisconnectMessage{peer=" + peer + "}";
    }

    public static ISerializer<DisconnectMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(DisconnectMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.peer, out);
        }

        @Override
        public DisconnectMessage deserialize(ByteBuf in) throws IOException {
            Host peer = Host.serializer.deserialize(in);
            return new DisconnectMessage(peer);
        }
    };
}