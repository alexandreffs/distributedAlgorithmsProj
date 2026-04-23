package protocols.membership.hyparview.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class NeighborReplyMessage extends ProtoMessage {

    public static final short MSG_ID = 605;

    private final boolean accepted;

    public NeighborReplyMessage(boolean accepted) {
        super(MSG_ID);
        this.accepted = accepted;
    }

    public boolean isAccepted() {
        return accepted;
    }

    @Override
    public String toString() {
        return "NeighborReplyMessage{" +
                "accepted=" + accepted +
                '}';
    }

    public static final ISerializer<NeighborReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(NeighborReplyMessage msg, ByteBuf out) throws IOException {
            out.writeBoolean(msg.accepted);
        }

        @Override
        public NeighborReplyMessage deserialize(ByteBuf in) throws IOException {
            boolean accepted = in.readBoolean();
            return new NeighborReplyMessage(accepted);
        }
    };
}