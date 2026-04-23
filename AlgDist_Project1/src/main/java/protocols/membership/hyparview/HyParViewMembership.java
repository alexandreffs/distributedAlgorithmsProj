package protocols.membership.hyparview;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import protocols.membership.hyparview.messages.DisconnectMessage;
import protocols.membership.hyparview.messages.ForwardJoinMessage;
import protocols.membership.hyparview.messages.JoinMessage;
import protocols.membership.hyparview.messages.NeighborMessage;
import protocols.membership.hyparview.messages.NeighborReplyMessage;
import protocols.membership.hyparview.messages.ShuffleMessage;
import protocols.membership.hyparview.messages.ShuffleReplyMessage;
import protocols.membership.hyparview.timers.ShuffleTimer;
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

public class HyParViewMembership extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(HyParViewMembership.class);

    public static final String PROTOCOL_NAME = "HyParViewMembership";
    public static final short PROTOCOL_ID = 600;

    // Views
    public static final String PAR_ACTIVE_VIEW_SIZE = "protocol.membership.hyparview.active_size";
    public static final String PAR_PASSIVE_VIEW_SIZE = "protocol.membership.hyparview.passive_size";

    // Join random walk
    public static final String PAR_ARWL = "protocol.membership.hyparview.arwl";
    public static final String PAR_PRWL = "protocol.membership.hyparview.prwl";

    // Shuffle
    public static final String PAR_SHUFFLE_TTL = "protocol.membership.hyparview.shuffle_ttl";
    public static final String PAR_SHUFFLE_PERIOD = "protocol.membership.hyparview.shuffle_period";
    public static final String PAR_K_ACTIVE = "protocol.membership.hyparview.shuffle_k_active";
    public static final String PAR_K_PASSIVE = "protocol.membership.hyparview.shuffle_k_passive";

    public static final String DEFAULT_ACTIVE_VIEW_SIZE = "4";
    public static final String DEFAULT_PASSIVE_VIEW_SIZE = "8";
    public static final String DEFAULT_ARWL = "4";
    public static final String DEFAULT_PRWL = "2";
    public static final String DEFAULT_SHUFFLE_TTL = "2";
    public static final String DEFAULT_SHUFFLE_PERIOD = "5000";
    public static final String DEFAULT_K_ACTIVE = "2";
    public static final String DEFAULT_K_PASSIVE = "3";

    private final Host self;

    private final Set<Host> activeView;
    private final Set<Host> passiveView;
    private final Set<Host> connectedPeers;
    private final Set<Host> pendingHighPriorityNeighbours;
    private final Set<Host> pendingLowPriorityNeighbours;
    private final Set<Host> pendingJoins;

    private final int activeViewSize;
    private final int passiveViewSize;
    private final int ARWL;
    private final int PRWL;

    private final int shuffleTTL;
    private final long shufflePeriod;
    private final int kActive;
    private final int kPassive;

    private final Random rnd;
    private final int channelId;

    public HyParViewMembership(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.self = self;
        this.activeView = new HashSet<>();
        this.passiveView = new HashSet<>();
        this.connectedPeers = new HashSet<>();
        this.pendingHighPriorityNeighbours = new HashSet<>();
        this.pendingLowPriorityNeighbours = new HashSet<>();
        this.pendingJoins = new HashSet<>();
        this.rnd = new Random();

        this.activeViewSize = Integer.parseInt(props.getProperty(PAR_ACTIVE_VIEW_SIZE, DEFAULT_ACTIVE_VIEW_SIZE));
        this.passiveViewSize = Integer.parseInt(props.getProperty(PAR_PASSIVE_VIEW_SIZE, DEFAULT_PASSIVE_VIEW_SIZE));
        this.ARWL = Integer.parseInt(props.getProperty(PAR_ARWL, DEFAULT_ARWL));
        this.PRWL = Integer.parseInt(props.getProperty(PAR_PRWL, DEFAULT_PRWL));

        this.shuffleTTL = Integer.parseInt(props.getProperty(PAR_SHUFFLE_TTL, DEFAULT_SHUFFLE_TTL));
        this.shufflePeriod = Long.parseLong(props.getProperty(PAR_SHUFFLE_PERIOD, DEFAULT_SHUFFLE_PERIOD));
        this.kActive = Integer.parseInt(props.getProperty(PAR_K_ACTIVE, DEFAULT_K_ACTIVE));
        this.kPassive = Integer.parseInt(props.getProperty(PAR_K_PASSIVE, DEFAULT_K_PASSIVE));

        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("babel.address"));
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("babel.port"));
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY,
                props.getProperty("channel_metrics_interval", "10000"));
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");

        channelId = createChannel(TCPChannel.NAME, channelProps);

        // Serializers
        registerMessageSerializer(channelId, JoinMessage.MSG_ID, JoinMessage.serializer);
        registerMessageSerializer(channelId, ForwardJoinMessage.MSG_ID, ForwardJoinMessage.serializer);
        registerMessageSerializer(channelId, DisconnectMessage.MSG_ID, DisconnectMessage.serializer);
        registerMessageSerializer(channelId, NeighborMessage.MSG_ID, NeighborMessage.serializer);
        registerMessageSerializer(channelId, NeighborReplyMessage.MSG_ID, NeighborReplyMessage.serializer);
        registerMessageSerializer(channelId, ShuffleMessage.MSG_ID, ShuffleMessage.serializer);
        registerMessageSerializer(channelId, ShuffleReplyMessage.MSG_ID, ShuffleReplyMessage.serializer);

        // Message handlers
        registerMessageHandler(channelId, JoinMessage.MSG_ID, this::uponJoin, this::uponMsgFail);
        registerMessageHandler(channelId, ForwardJoinMessage.MSG_ID, this::uponForwardJoin, this::uponMsgFail);
        registerMessageHandler(channelId, DisconnectMessage.MSG_ID, this::uponDisconnect, this::uponMsgFail);
        registerMessageHandler(channelId, NeighborMessage.MSG_ID, this::uponNeighbor, this::uponMsgFail);
        registerMessageHandler(channelId, NeighborReplyMessage.MSG_ID, this::uponNeighborReply, this::uponMsgFail);
        registerMessageHandler(channelId, ShuffleMessage.MSG_ID, this::uponShuffle, this::uponMsgFail);
        registerMessageHandler(channelId, ShuffleReplyMessage.MSG_ID, this::uponShuffleReply, this::uponMsgFail);

        // Channel events
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        // Timer handlers
        registerTimerHandler(ShuffleTimer.TIMER_ID, this::uponShuffleTimer);
    }

    @Override
    public void init(Properties props) {
        triggerNotification(new ChannelCreated(channelId));

        setupPeriodicTimer(new ShuffleTimer(), shufflePeriod, shufflePeriod);

        if (props.containsKey("contact")) {
            try {
                String[] hostElems = props.getProperty("contact").split(":");
                Host contactNode = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));

                if (!contactNode.equals(self)) {
                    addNodeActiveView(contactNode);
                    pendingJoins.add(contactNode);
                }

            } catch (Exception e) {
                logger.error("Invalid contact: {}", props.getProperty("contact"));
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    /* -------------------------------------------------------------------------- */
    /* JOIN / FORWARD_JOIN / DISCONNECT */
    /* -------------------------------------------------------------------------- */

    private void uponJoin(JoinMessage msg, Host from, short sourceProto, int channelId) {
        Host newNode = msg.getNewNode();
        logger.info("Received {} from {}", msg, from);

        if (isActiveViewFull()) {
            dropRandomElementFromActiveView();
        }

        addNodeActiveView(newNode);

        for (Host n : new HashSet<>(activeView)) {
            if (!n.equals(newNode) && connectedPeers.contains(n)) {
                sendMessage(new ForwardJoinMessage(newNode, ARWL), n);
            }
        }
    }

    private void uponForwardJoin(ForwardJoinMessage msg, Host from, short sourceProto, int channelId) {
        Host newNode = msg.getNewNode();
        int ttl = msg.getTimeToLive();

        logger.info("Received {} from {}", msg, from);

        if (newNode.equals(self))
            return;

        if (ttl == 0 || activeView.size() == 1) {
            addNodeActiveView(newNode);
        } else {
            if (ttl == PRWL) {
                addNodePassiveView(newNode);
            }

            Host next = getRandomNodeExcluding(activeView, from);
            if (next != null && connectedPeers.contains(next)) {
                sendMessage(new ForwardJoinMessage(newNode, ttl - 1), next);
            }
        }
    }

    private void uponDisconnect(DisconnectMessage msg, Host from, short sourceProto, int channelId) {
        Host peer = from;
        logger.info("Received {} from {}", msg, from);

        pendingJoins.remove(peer);
        pendingHighPriorityNeighbours.remove(peer);
        pendingLowPriorityNeighbours.remove(peer);

        if (activeView.remove(peer)) {
            if (connectedPeers.remove(peer)) {
                triggerNotification(new NeighbourDown(peer));
            }
            addNodePassiveView(peer);
            attemptActiveViewRepair();
        }
    }

    /* -------------------------------------------------------------------------- */
    /* NEIGHBOR */
    /* -------------------------------------------------------------------------- */

    private void uponNeighbor(NeighborMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);

        boolean accepted = false;

        if (from.equals(self)) {
            sendMessage(new NeighborReplyMessage(false), from);
            return;
        }

        if (msg.isHighPriority()) {
            if (isActiveViewFull()) {
                dropRandomElementFromActiveView();
            }
            addNodeActiveView(from);
            accepted = true;
        } else {
            if (!isActiveViewFull()) {
                addNodeActiveView(from);
                accepted = true;
            }
        }

        sendMessage(new NeighborReplyMessage(accepted), from);
    }

    private void uponNeighborReply(NeighborReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);

        if (msg.isAccepted()) {
            addNodeActiveView(from);
        } else {
            activeView.remove(from);
            connectedPeers.remove(from);
            addNodePassiveView(from);
            attemptActiveViewRepair();
        }
    }

    /* -------------------------------------------------------------------------- */
    /* SHUFFLE */
    /* -------------------------------------------------------------------------- */

    private void uponShuffle(ShuffleMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);

        int ttl = msg.getTtl();

        if (ttl == 0 || activeView.size() <= 1) {
            Set<Host> replySample = buildShuffleReplySample();
            sendMessage(new ShuffleReplyMessage(replySample), msg.getOrigin());

            mergeIntoPassiveView(msg.getSample(), msg.getOrigin());
        } else {
            Host next = getRandomNodeExcluding(activeView, from);
            if (next != null) {
                sendMessage(new ShuffleMessage(msg.getOrigin(), msg.getSample(), ttl - 1), next);
            }
        }
    }

    private void uponShuffleReply(ShuffleReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        mergeIntoPassiveView(msg.getSample(), from);
    }

    private void uponShuffleTimer(ShuffleTimer timer, long timerId) {
        if (activeView.isEmpty())
            return;

        Host target = getRandomNode(activeView);
        if (target == null)
            return;

        if (!connectedPeers.contains(target))
            return;

        Set<Host> sample = buildShuffleSample();
        sendMessage(new ShuffleMessage(self, sample, shuffleTTL), target);
    }

    /* -------------------------------------------------------------------------- */
    /* AUX */
    /* -------------------------------------------------------------------------- */
    private void attemptActiveViewRepair() {
        if (activeView.size() >= activeViewSize)
            return;
        if (passiveView.isEmpty())
            return;

        while (activeView.size() < activeViewSize && !passiveView.isEmpty()) {
            Host candidate = getRandomNode(passiveView);
            if (candidate == null)
                return;

            passiveView.remove(candidate);
            addNodeActiveView(candidate);
            pendingHighPriorityNeighbours.add(candidate);
        }
    }

    private void dropRandomElementFromActiveView() {
        Host n = getRandomNode(activeView);
        if (n == null)
            return;

        if (connectedPeers.contains(n)) {
            sendMessage(new DisconnectMessage(), n);
        }

        activeView.remove(n);
        if (connectedPeers.remove(n)) {
            triggerNotification(new NeighbourDown(n));
        }
        addNodePassiveView(n);
    }

    private void addNodeActiveView(Host node) {
        if (node.equals(self))
            return;
        if (activeView.contains(node))
            return;

        if (isActiveViewFull()) {
            dropRandomElementFromActiveView();
        }

        passiveView.remove(node);
        activeView.add(node);
        openConnection(node);
    }

    private void addNodePassiveView(Host node) {
        if (node.equals(self))
            return;
        if (activeView.contains(node))
            return;
        if (passiveView.contains(node))
            return;

        if (isPassiveViewFull()) {
            Host toRemove = getRandomNode(passiveView);
            if (toRemove != null)
                passiveView.remove(toRemove);
        }

        passiveView.add(node);
    }

    private void mergeIntoPassiveView(Set<Host> hosts, Host otherPeer) {
        for (Host h : hosts) {
            if (h.equals(self))
                continue;
            if (activeView.contains(h))
                continue;
            addNodePassiveView(h);
        }

        // opcionalmente dar preferência a remover entradas antigas/peer remoto
        if (otherPeer != null && !otherPeer.equals(self) && !activeView.contains(otherPeer)) {
            addNodePassiveView(otherPeer);
        }
    }

    private Set<Host> buildShuffleSample() {
        Set<Host> sample = new HashSet<>();
        sample.add(self);

        List<Host> activeCandidates = new ArrayList<>(activeView);
        activeCandidates.remove(self);
        shuffleList(activeCandidates);

        int countA = 0;
        for (Host h : activeCandidates) {
            if (countA >= kActive)
                break;
            sample.add(h);
            countA++;
        }

        List<Host> passiveCandidates = new ArrayList<>(passiveView);
        shuffleList(passiveCandidates);

        int countP = 0;
        for (Host h : passiveCandidates) {
            if (countP >= kPassive)
                break;
            sample.add(h);
            countP++;
        }

        return sample;
    }

    private Set<Host> buildShuffleReplySample() {
        Set<Host> sample = new HashSet<>();
        List<Host> passiveCandidates = new ArrayList<>(passiveView);
        shuffleList(passiveCandidates);

        int max = Math.min(kActive + kPassive, passiveCandidates.size());
        for (int i = 0; i < max; i++) {
            sample.add(passiveCandidates.get(i));
        }

        return sample;
    }

    private void shuffleList(List<Host> list) {
        for (int i = list.size() - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            Host tmp = list.get(i);
            list.set(i, list.get(j));
            list.set(j, tmp);
        }
    }

    private boolean isActiveViewFull() {
        return activeView.size() >= activeViewSize;
    }

    private boolean isPassiveViewFull() {
        return passiveView.size() >= passiveViewSize;
    }

    private Host getRandomNode(Set<Host> view) {
        if (view.isEmpty())
            return null;

        List<Host> list = new ArrayList<>(view);
        return list.get(rnd.nextInt(list.size()));
    }

    private Host getRandomNodeExcluding(Set<Host> view, Host exclude) {
        List<Host> list = new ArrayList<>();

        for (Host h : view) {
            if (!h.equals(exclude)) {
                list.add(h);
            }
        }

        if (list.isEmpty())
            return null;
        return list.get(rnd.nextInt(list.size()));
    }

    /* -------------------------------------------------------------------------- */
    /* CHANNEL EVENTS */
    /* -------------------------------------------------------------------------- */
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} is up", peer);

        if (activeView.contains(peer) && connectedPeers.add(peer)) {
            triggerNotification(new NeighbourUp(peer));
        }

        if (pendingJoins.remove(peer)) {
            sendMessage(new JoinMessage(self), peer);
        }

        if (pendingHighPriorityNeighbours.remove(peer)) {
            sendMessage(new NeighborMessage(true), peer);
        }

        if (pendingLowPriorityNeighbours.remove(peer)) {
            sendMessage(new NeighborMessage(false), peer);
        }
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} is down cause {}", peer, event.getCause());

        pendingJoins.remove(peer);
        pendingHighPriorityNeighbours.remove(peer);
        pendingLowPriorityNeighbours.remove(peer);

        if (activeView.remove(peer)) {
            if (connectedPeers.remove(peer)) {
                triggerNotification(new NeighbourDown(peer));
            }
            addNodePassiveView(peer);
            attemptActiveViewRepair();
        }
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} failed cause: {}", peer, event.getCause());

        pendingJoins.remove(peer);
        pendingHighPriorityNeighbours.remove(peer);
        pendingLowPriorityNeighbours.remove(peer);

        activeView.remove(peer);
        connectedPeers.remove(peer);
        addNodePassiveView(peer);
        attemptActiveViewRepair();
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.info("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection from {} is down, cause: {}", peer, event.getCause());

        if (activeView.remove(peer)) {
            if (connectedPeers.remove(peer)) {
                triggerNotification(new NeighbourDown(peer));
            }
            addNodePassiveView(peer);
            attemptActiveViewRepair();
        }
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }
}