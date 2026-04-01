package protocols.membership.hyparview.messages;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class ShuffleMessage extends ProtoMessage {

    public static final short MSG_ID = 606;

    private final Host origin;
    private final Set<Host> sample;
    private final int ttl;

    public ShuffleMessage(Host origin, Set<Host> sample, int ttl) {
        super(MSG_ID);
        this.origin = origin;
        this.sample = sample;
        this.ttl = ttl;
    }

    public Host getOrigin() {
        return origin;
    }

    public Set<Host> getSample() {
        return sample;
    }

    public int getTtl() {
        return ttl;
    }

    @Override
    public String toString() {
        return "ShuffleMessage{" +
                "origin=" + origin +
                ", sample=" + sample +
                ", ttl=" + ttl +
                '}';
    }

    public static final ISerializer<ShuffleMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ShuffleMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.origin, out);

            out.writeInt(msg.sample.size());
            for (Host h : msg.sample) {
                Host.serializer.serialize(h, out);
            }

            out.writeInt(msg.ttl);
        }

        @Override
        public ShuffleMessage deserialize(ByteBuf in) throws IOException {
            Host origin = Host.serializer.deserialize(in);

            int size = in.readInt();
            Set<Host> sample = new HashSet<>();
            for (int i = 0; i < size; i++) {
                sample.add(Host.serializer.deserialize(in));
            }

            int ttl = in.readInt();

            return new ShuffleMessage(origin, sample, ttl);
        }
    };
}