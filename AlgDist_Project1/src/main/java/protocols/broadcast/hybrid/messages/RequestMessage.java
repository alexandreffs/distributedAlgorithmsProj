package protocols.broadcast.hybrid.messages;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class RequestMessage extends ProtoMessage {
    public static final short MSG_ID = 703;

    private final UUID mid;
    private final Host requester;

    public RequestMessage(UUID mid, Host requester) {
        super(MSG_ID);
        this.mid = mid;
        this.requester = requester;
    }

    public UUID getMid() {
        return mid;
    }

    public Host getRequester() {
        return requester;
    }

    @Override
    public String toString() {
        return "RequestMessage{" +
                "mid=" + mid +
                '}';
    }

    public static ISerializer<RequestMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(RequestMessage msg, ByteBuf out) throws IOException {
            out.writeLong(msg.mid.getMostSignificantBits());
            out.writeLong(msg.mid.getLeastSignificantBits());
            Host.serializer.serialize(msg.requester, out);
        }

        @Override
        public RequestMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host requester = Host.serializer.deserialize(in);

            return new RequestMessage(mid, requester);
        }
    };
}