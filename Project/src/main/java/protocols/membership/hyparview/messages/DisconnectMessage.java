package protocols.membership.hyparview.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class DisconnectMessage extends ProtoMessage {

    public static final short MSG_ID = 603;

    public DisconnectMessage() {
        super(MSG_ID);
    }

    @Override
    public String toString() {
        return "DisconnectMessage{}";
    }

    public static final ISerializer<DisconnectMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(DisconnectMessage msg, ByteBuf out) throws IOException {
            // nothing to serialize
        }

        @Override
        public DisconnectMessage deserialize(ByteBuf in) throws IOException {
            return new DisconnectMessage();
        }
    };
}