package protocols.membership.hyparview.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class NeighborMessage extends ProtoMessage {

    public static final short MSG_ID = 604;

    private final boolean highPriority;

    public NeighborMessage(boolean highPriority) {
        super(MSG_ID);
        this.highPriority = highPriority;
    }

    public boolean isHighPriority() {
        return highPriority;
    }

    @Override
    public String toString() {
        return "NeighborMessage{" +
                "highPriority=" + highPriority +
                '}';
    }

    public static final ISerializer<NeighborMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(NeighborMessage msg, ByteBuf out) throws IOException {
            out.writeBoolean(msg.highPriority);
        }

        @Override
        public NeighborMessage deserialize(ByteBuf in) throws IOException {
            boolean highPriority = in.readBoolean();
            return new NeighborMessage(highPriority);
        }
    };
}