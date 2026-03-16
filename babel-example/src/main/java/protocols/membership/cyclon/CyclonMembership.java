package protocols.membership.cyclon;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import protocols.membership.cyclon.messages.ShuffleReply;
import protocols.membership.cyclon.messages.ShuffleRequest;
import protocols.membership.cyclon.timers.ShuffleTimer;

import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionUp;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;

public class CyclonMembership extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(CyclonMembership.class);

    public static final short PROTOCOL_ID = 102;
    public static final String PROTOCOL_NAME = "CyclonMembership";

    private final Host self;
    private final Map<Host, Integer> neigh;
    private final Set<Host> pending;
    private final Set<Host> sample;

    private final int maxN;
    private final int shuffleTime;
    private final int subsetSize;

    private final Random rnd;
    private final int channelId;

    public static final String PAR_MAX_NEIGH = "protocol.membership.cyclon.maxN";
    public static final String PAR_DEFAULT_MAX_NEIGH = "6";

    public static final String PAR_SHUFFLE_TIME = "protocol.membership.cyclon.shuffle_time";
    public static final String PAR_DEFAULT_SHUFFLE_TIME = "2000";

    public static final String PAR_SUBSET_SIZE = "protocol.membership.cyclon.subset_size";
    public static final String PAR_DEFAULT_SUBSET_SIZE = "5";

    public CyclonMembership(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.self = self;
        this.neigh = new HashMap<>();
        this.pending = new HashSet<>();
        this.sample = new HashSet<>();
        this.rnd = new Random();

        this.maxN = Integer.parseInt(props.getProperty(PAR_MAX_NEIGH, PAR_DEFAULT_MAX_NEIGH));
        this.shuffleTime = Integer.parseInt(props.getProperty(PAR_SHUFFLE_TIME, PAR_DEFAULT_SHUFFLE_TIME));
        this.subsetSize = Integer.parseInt(props.getProperty(PAR_SUBSET_SIZE, PAR_DEFAULT_SUBSET_SIZE));

        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address"));
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port"));
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY,
                props.getProperty("channel_metrics_interval", "10000"));
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");

        channelId = createChannel(TCPChannel.NAME, channelProps);

        registerMessageSerializer(channelId, ShuffleRequest.MSG_ID, ShuffleRequest.serializer);
        registerMessageSerializer(channelId, ShuffleReply.MSG_ID, ShuffleReply.serializer);

        registerMessageHandler(channelId, ShuffleRequest.MSG_ID, this::uponShuffleRequest, this::uponMsgFail);
        registerMessageHandler(channelId, ShuffleReply.MSG_ID, this::uponShuffleReply, this::uponMsgFail);

        registerTimerHandler(ShuffleTimer.TIMER_ID, this::uponShuffleTimer);

        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
    }

    @Override
    public void init(Properties props) {
        triggerNotification(new ChannelCreated(channelId));

        if (props.containsKey("contact")) {
            try {
                String[] hostElems = props.getProperty("contact").split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                pending.add(contactHost);
                openConnection(contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact: {}", props.getProperty("contact"));
                e.printStackTrace();
                System.exit(-1);
            }
        }

        setupPeriodicTimer(new ShuffleTimer(), shuffleTime, shuffleTime);
    }

    private void uponShuffleRequest(ShuffleRequest msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);

        Set<Host> temporarySample = randomSubset(subsetSize);
        sendMessage(new ShuffleReply(temporarySample), from);

        mergeViews(msg.getSample(), temporarySample);
    }

    private void uponShuffleReply(ShuffleReply msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        mergeViews(msg.getSample(), sample);
    }

    private void mergeViews(Set<Host> peerSample, Set<Host> mySample) {
        for (Host h : peerSample) {
            if (h.equals(self))
                continue;

            if (neigh.containsKey(h)) {
                continue;
            }

            if (neigh.size() < maxN) {
                neigh.put(h, 0);
                triggerNotification(new NeighbourUp(h));
                if (!pending.contains(h)) {
                    pending.add(h);
                    openConnection(h);
                }
            } else {
                Host replacement = null;

                for (Host x : mySample) {
                    if (neigh.containsKey(x)) {
                        replacement = x;
                        break;
                    }
                }

                if (replacement == null) {
                    replacement = getRandom(neigh.keySet());
                }

                if (replacement != null) {
                    neigh.remove(replacement);
                    triggerNotification(new NeighbourDown(replacement));

                    neigh.put(h, 0);
                    triggerNotification(new NeighbourUp(h));

                    if (!pending.contains(h)) {
                        pending.add(h);
                        openConnection(h);
                    }
                }
            }
        }
    }

    private Host getRandom(Set<Host> hosts) {
        if (hosts.isEmpty())
            return null;

        int idx = rnd.nextInt(hosts.size());
        int i = 0;
        for (Host h : hosts) {
            if (i == idx)
                return h;
            i++;
        }
        return null;
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponShuffleTimer(ShuffleTimer timer, long timerId) {
        logger.debug("Shuffle timer triggered");

        if (neigh.isEmpty())
            return;

        increaseAges();

        Host oldest = pickOldest();
        if (oldest == null)
            return;

        neigh.remove(oldest);

        Set<Host> subset = randomSubset(subsetSize - 1);

        sample.clear();
        sample.addAll(subset);

        Set<Host> toSend = new HashSet<>(subset);
        toSend.add(self);

        sendMessage(new ShuffleRequest(toSend), oldest);
        logger.debug("Sent ShuffleRequest to {} with sample {}", oldest, toSend);
    }

    private void increaseAges() {
        List<Host> hosts = new ArrayList<>(neigh.keySet());
        for (Host h : hosts) {
            neigh.put(h, neigh.get(h) + 1);
        }
    }

    private Host pickOldest() {
        Host oldest = null;
        int maxAge = -1;

        for (Map.Entry<Host, Integer> entry : neigh.entrySet()) {
            if (entry.getValue() > maxAge) {
                maxAge = entry.getValue();
                oldest = entry.getKey();
            }
        }
        return oldest;
    }

    private Set<Host> randomSubset(int size) {
        List<Host> list = new ArrayList<>(neigh.keySet());
        Collections.shuffle(list, rnd);
        return new HashSet<>(list.subList(0, Math.min(size, list.size())));
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} is up", peer);
        pending.remove(peer);

        if (!neigh.containsKey(peer)) {
            neigh.put(peer, 0);
            triggerNotification(new NeighbourUp(peer));
        }
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} is down cause {}", peer, event.getCause());

        if (neigh.remove(peer) != null) {
            triggerNotification(new NeighbourDown(peer));
        }
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.debug("Connection to {} failed cause: {}", event.getNode(), event.getCause());
        pending.remove(event.getNode());
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }
}