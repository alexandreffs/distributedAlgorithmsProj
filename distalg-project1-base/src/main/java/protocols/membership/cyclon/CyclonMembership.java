package protocols.membership.cyclon;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

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

    public static final short PROTOCOL_ID = 500;
    public static final String PROTOCOL_NAME = "CyclonMembership";

    public static final String PAR_MAX_NEIGH = "protocol.membership.cyclon.maxN";
    public static final String PAR_DEFAULT_MAX_NEIGH = "6";

    public static final String PAR_SHUFFLE_TIME = "protocol.membership.cyclon.shuffle_time";
    public static final String PAR_DEFAULT_SHUFFLE_TIME = "2000";

    public static final String PAR_SUBSET_SIZE = "protocol.membership.cyclon.subset_size";
    public static final String PAR_DEFAULT_SUBSET_SIZE = "5";

    private final Host self;

    // Partial view: Host -> age
    private final Map<Host, Integer> neigh;

    // Peers to which we are currently trying to open an outgoing connection
    private final Set<Host> pending;

    // Stores the sample we sent to each peer, so that when the reply arrives
    // we can do mergeViews(peerSample, mySampleSentToThatPeer)
    private final Map<Host, Map<Host, Integer>> sentSamples;

    // Tracks peers for which we already notified NeighbourUp
    private final Set<Host> connectedPeers;

    private final int maxN;
    private final int shuffleTime;
    private final int subsetSize;

    private final Random rnd;
    private final int channelId;

    public CyclonMembership(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.self = self;
        this.neigh = new HashMap<>();
        this.pending = new HashSet<>();
        this.sentSamples = new HashMap<>();
        this.connectedPeers = new HashSet<>();
        this.rnd = new Random();

        this.maxN = Integer.parseInt(props.getProperty(PAR_MAX_NEIGH, PAR_DEFAULT_MAX_NEIGH));
        this.shuffleTime = Integer.parseInt(props.getProperty(PAR_SHUFFLE_TIME, PAR_DEFAULT_SHUFFLE_TIME));
        this.subsetSize = Integer.parseInt(props.getProperty(PAR_SUBSET_SIZE, PAR_DEFAULT_SUBSET_SIZE));

        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("babel.address"));
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("babel.port"));
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY,
                props.getProperty("channel_metrics_interval", "10000"));
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");

        channelId = createChannel(TCPChannel.NAME, channelProps);

        /* ---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, ShuffleRequest.MSG_ID, ShuffleRequest.serializer);
        registerMessageSerializer(channelId, ShuffleReply.MSG_ID, ShuffleReply.serializer);

        /* ---------------------- Register Message Handlers ------------------------- */
        registerMessageHandler(channelId, ShuffleRequest.MSG_ID, this::uponShuffleRequest, this::uponMsgFail);
        registerMessageHandler(channelId, ShuffleReply.MSG_ID, this::uponShuffleReply, this::uponMsgFail);

        /* ---------------------- Register Timer Handlers -------------------------- */
        registerTimerHandler(ShuffleTimer.TIMER_ID, this::uponShuffleTimer);

        /* ---------------------- Register Channel Event Handlers ------------------ */
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

                if (!contactHost.equals(self)) {
                    neigh.put(contactHost, 0);
                    if (!pending.contains(contactHost)) {
                        pending.add(contactHost);
                        openConnection(contactHost);
                    }
                }
            } catch (Exception e) {
                logger.error("Invalid contact: {}", props.getProperty("contact"));
                e.printStackTrace();
                System.exit(-1);
            }
        }

        setupPeriodicTimer(new ShuffleTimer(), shuffleTime, shuffleTime);
    }

    /*
     * --------------------------------- Messages ---------------------------------
     */

    private void uponShuffleRequest(ShuffleRequest msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);

        // Ensure sender is known in view
        if (!from.equals(self)) {
            if (!neigh.containsKey(from) && neigh.size() < maxN) {
                neigh.put(from, 0);
                tryConnectIfNeeded(from);
            } else if (neigh.containsKey(from)) {
                // if talked to this node, set age to 0
                neigh.put(from, 0);
            }
        }

        // Select random sample from my current view
        Map<Host, Integer> temporarySample = randomSubset(subsetSize, from);

        // Reply with that sample
        sendMessage(new ShuffleReply(temporarySample), from);

        // Merge their sample into my view, preferring to replace entries from the
        // sample I just sent
        mergeViews(msg.getSample(), temporarySample);
    }

    private void uponShuffleReply(ShuffleReply msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);

        if (neigh.containsKey(from)) {
            neigh.put(from, 0);
        }

        Map<Host, Integer> mySample = sentSamples.remove(from);
        if (mySample == null) {
            logger.warn("Received ShuffleReply from {} but no stored sent sample exists", from);
            return;
        }

        mergeViews(msg.getSample(), mySample);
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /*
     * --------------------------------- Timers -----------------------------------
     */

    private void uponShuffleTimer(ShuffleTimer timer, long timerId) {
        logger.info("Shuffle timer triggered. View: {}", neigh);

        if (neigh.isEmpty())
            return;

        increaseAges();

        Host oldest = pickOldest();
        if (oldest == null)
            return;

        // Create sample excluding oldest
        Map<Host, Integer> subset = randomSubset(Math.max(0, subsetSize - 1), oldest);

        // Store sample sent later for merge when reply arrives
        Map<Host, Integer> mySample = new HashMap<>(subset);
        mySample.put(self, 0);

        sentSamples.put(oldest, new HashMap<>(mySample));

        sendMessage(new ShuffleRequest(mySample), oldest);
        logger.info("Sent ShuffleRequest to {} with sample {}", oldest, mySample);
    }

    /*
     * --------------------------------- Cyclon Logic -----------------------------
     */

    private void increaseAges() {
        List<Host> hosts = new ArrayList<>(neigh.keySet());
        for (Host h : hosts) {
            neigh.put(h, neigh.get(h) + 1);
        }
    }

    private Host pickOldest() {
        int maxAge = -1;
        List<Host> candidates = new ArrayList<>();

        for (Map.Entry<Host, Integer> entry : neigh.entrySet()) {
            int age = entry.getValue();

            if (age > maxAge) {
                maxAge = age;
                candidates.clear();
                candidates.add(entry.getKey());
            } else if (age == maxAge) {
                candidates.add(entry.getKey());
            }
        }

        if (candidates.isEmpty())
            return null;

        return candidates.get(rnd.nextInt(candidates.size()));
    }

    private Map<Host, Integer> randomSubset(int size, Host exclude) {
        List<Map.Entry<Host, Integer>> entries = new ArrayList<>(neigh.entrySet());

        if (exclude != null) {
            entries.removeIf(e -> e.getKey().equals(exclude));
        }

        Collections.shuffle(entries, rnd);

        Map<Host, Integer> result = new HashMap<>();
        int limit = Math.min(size, entries.size());

        for (int i = 0; i < limit; i++) {
            Map.Entry<Host, Integer> e = entries.get(i);
            result.put(e.getKey(), e.getValue());
        }

        return result;
    }

    private void mergeViews(Map<Host, Integer> peerSample, Map<Host, Integer> mySample) {
        for (Map.Entry<Host, Integer> entry : peerSample.entrySet()) {
            Host p = entry.getKey();
            int age = entry.getValue();

            if (p.equals(self))
                continue;

            // If already in view, keep the freshest age
            if (neigh.containsKey(p)) {
                if (age < neigh.get(p)) {
                    neigh.put(p, age);
                }
                continue;
            }

            // If there is space, just add
            if (neigh.size() < maxN) {
                neigh.put(p, age);
                tryConnectIfNeeded(p);
                continue;
            }

            // Otherwise, replace an element that belongs to mySample, if possible
            Host replacement = pickReplacementFromMySample(mySample);

            if (replacement != null) {
                removePeerFromView(replacement);
                neigh.put(p, age);
                tryConnectIfNeeded(p);
            }
        }
    }

    private Host pickReplacementFromMySample(Map<Host, Integer> mySample) {
        List<Host> candidates = new ArrayList<>();

        for (Host h : mySample.keySet()) {
            if (!h.equals(self) && neigh.containsKey(h)) {
                candidates.add(h);
            }
        }

        if (candidates.isEmpty())
            return null;

        return candidates.get(rnd.nextInt(candidates.size()));
    }

    private void tryConnectIfNeeded(Host h) {
        if (h.equals(self))
            return;

        if (!pending.contains(h) && !connectedPeers.contains(h)) {
            pending.add(h);
            openConnection(h);
        }
    }

    private void removePeerFromView(Host peer) {
        neigh.remove(peer);
        pending.remove(peer);
        sentSamples.remove(peer);

        if (connectedPeers.remove(peer)) {
            triggerNotification(new NeighbourDown(peer));
        }

        closeConnection(peer);
    }

    /*
     * ------------------------------- TCPChannel Events ---------------------------
     */

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} is up", peer);

        pending.remove(peer);

        // If connection succeeded to a peer not yet in view, add it
        neigh.put(peer, 0);

        if (connectedPeers.add(peer)) {
            triggerNotification(new NeighbourUp(peer));
        }
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} is down cause {}", peer, event.getCause());

        neigh.remove(peer);
        pending.remove(peer);
        sentSamples.remove(peer);

        if (connectedPeers.remove(peer)) {
            triggerNotification(new NeighbourDown(peer));
        }
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} failed cause: {}", peer, event.getCause());

        pending.remove(peer);

        // Optional policy:
        // if connection never came up, remove from view
        if (!connectedPeers.contains(peer)) {
            neigh.remove(peer);
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.info("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.info("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }
}