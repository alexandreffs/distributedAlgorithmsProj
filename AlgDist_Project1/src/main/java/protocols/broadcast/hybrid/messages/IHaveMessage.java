package protocols.broadcast.hybrid.messages;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class IHaveMessage extends ProtoMessage {
    public static final short MSG_ID = 702;

    private final UUID mid;
    private final Host announcer;

    public IHaveMessage(UUID mid, Host announcer) {
        super(MSG_ID);
        this.mid = mid;
        this.announcer = announcer;
    }

    public UUID getMid() {
        return mid;
    }

    public Host getAnnouncer() {
        return announcer;
    }

    @Override
    public String toString() {
        return "IHaveMessage{" +
                "mid=" + mid +
                '}';
    }

    public static ISerializer<IHaveMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(IHaveMessage msg, ByteBuf out) throws IOException {
            out.writeLong(msg.mid.getMostSignificantBits());
            out.writeLong(msg.mid.getLeastSignificantBits());
            Host.serializer.serialize(msg.announcer, out);
        }

        @Override
        public IHaveMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host announcer = Host.serializer.deserialize(in);

            return new IHaveMessage(mid, announcer);
        }
    };
}