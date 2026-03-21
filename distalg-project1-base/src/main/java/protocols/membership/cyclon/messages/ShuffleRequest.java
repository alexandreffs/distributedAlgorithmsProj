package protocols.membership.cyclon.messages;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class ShuffleRequest extends ProtoMessage {

    public static final short MSG_ID = 502;

    private final Map<Host, Integer> sample;

    public ShuffleRequest(Map<Host, Integer> sample) {
        super(MSG_ID);
        this.sample = sample;
    }

    public Map<Host, Integer> getSample() {
        return sample;
    }

    public static ISerializer<ShuffleRequest> serializer = new ISerializer<ShuffleRequest>() {
        @Override
        public void serialize(ShuffleRequest msg, ByteBuf out) throws IOException {
            out.writeInt(msg.sample.size());
            for (Map.Entry<Host, Integer> entry : msg.sample.entrySet()) {
                Host.serializer.serialize(entry.getKey(), out);
                out.writeInt(entry.getValue());
            }
        }

        @Override
        public ShuffleRequest deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            Map<Host, Integer> sample = new HashMap<>();

            for (int i = 0; i < size; i++) {
                Host h = Host.serializer.deserialize(in);
                int age = in.readInt();
                sample.put(h, age);
            }

            return new ShuffleRequest(sample);
        }
    };

    @Override
    public String toString() {
        return "ShuffleRequest{" +
                "sample=" + sample +
                '}';
    }
}