package protocols.membership.cyclon.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ShuffleReply extends ProtoMessage {

    public static final short MSG_ID = 302;

    private final Set<Host> sample;

    public ShuffleReply(Set<Host> sample) {
        super(MSG_ID);
        this.sample = sample;
    }

    public Set<Host> getSample() {
        return sample;
    }

    public static ISerializer<ShuffleReply> serializer = new ISerializer<ShuffleReply>() {
        @Override
        public void serialize(ShuffleReply msg, ByteBuf out) throws IOException {
            out.writeInt(msg.sample.size());
            for (Host h : msg.sample)
                Host.serializer.serialize(h, out);
        }

        @Override
        public ShuffleReply deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            Set<Host> sample = new HashSet<>();
            for (int i = 0; i < size; i++)
                sample.add(Host.serializer.deserialize(in));
            return new ShuffleReply(sample);
        }
    };
}