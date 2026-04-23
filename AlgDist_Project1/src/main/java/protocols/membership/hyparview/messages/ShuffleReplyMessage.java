package protocols.membership.hyparview.messages;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class ShuffleReplyMessage extends ProtoMessage {

    public static final short MSG_ID = 607;

    private final Set<Host> sample;

    public ShuffleReplyMessage(Set<Host> sample) {
        super(MSG_ID);
        this.sample = sample;
    }

    public Set<Host> getSample() {
        return sample;
    }

    @Override
    public String toString() {
        return "ShuffleReplyMessage{" +
                "sample=" + sample +
                '}';
    }

    public static final ISerializer<ShuffleReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ShuffleReplyMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.sample.size());
            for (Host h : msg.sample) {
                Host.serializer.serialize(h, out);
            }
        }

        @Override
        public ShuffleReplyMessage deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            Set<Host> sample = new HashSet<>();

            for (int i = 0; i < size; i++) {
                sample.add(Host.serializer.deserialize(in));
            }

            return new ShuffleReplyMessage(sample);
        }
    };
}