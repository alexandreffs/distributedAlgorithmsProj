package protocols.broadcast.hybrid.messages;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class GossipFullMessage extends ProtoMessage {
    public static final short MSG_ID = 701;

    private final UUID mid;
    private final Host sender;
    private final short toDeliver;
    private final byte[] content;

    public GossipFullMessage(UUID mid, Host sender, short toDeliver, byte[] content) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.toDeliver = toDeliver;
        this.content = content;
    }

    public UUID getMid() {
        return mid;
    }

    public Host getSender() {
        return sender;
    }

    public short getToDeliver() {
        return toDeliver;
    }

    public byte[] getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "GossipFullMessage{" +
                "mid=" + mid +
                '}';
    }

    public static ISerializer<GossipFullMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(GossipFullMessage msg, ByteBuf out) throws IOException {
            out.writeLong(msg.mid.getMostSignificantBits());
            out.writeLong(msg.mid.getLeastSignificantBits());
            Host.serializer.serialize(msg.sender, out);
            out.writeShort(msg.toDeliver);
            out.writeInt(msg.content.length);
            if (msg.content.length > 0) {
                out.writeBytes(msg.content);
            }
        }

        @Override
        public GossipFullMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);

            Host sender = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();

            int size = in.readInt();
            byte[] content = new byte[size];
            if (size > 0) {
                in.readBytes(content);
            }

            return new GossipFullMessage(mid, sender, toDeliver, content);
        }
    };
}