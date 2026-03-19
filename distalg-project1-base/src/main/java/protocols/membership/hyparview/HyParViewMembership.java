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

    public static final String PAR_ACTIVE_VIEW_SIZE = "protocol.membership.hyparview.active_size";
    public static final String PAR_PASSIVE_VIEW_SIZE = "protocol.membership.hyparview.passive_size";
    public static final String PAR_ARWL = "protocol.membership.hyparview.arwl";
    public static final String PAR_PRWL = "protocol.membership.hyparview.prwl";

    public static final String DEFAULT_ACTIVE_VIEW_SIZE = "4";
    public static final String DEFAULT_PASSIVE_VIEW_SIZE = "8";
    public static final String DEFAULT_ARWL = "4";
    public static final String DEFAULT_PRWL = "2";

    private final Host self;

    private final Set<Host> activeView;
    private final Set<Host> passiveView;
    private final Set<Host> connectedPeers;

    private final int activeViewSize;
    private final int passiveViewSize;
    private final int ARWL;
    private final int PRWL;

    private final Random rnd;
    private final int channelId;

    public HyParViewMembership(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.self = self;
        this.activeView = new HashSet<>();
        this.passiveView = new HashSet<>();
        this.connectedPeers = new HashSet<>();
        this.rnd = new Random();

        this.activeViewSize = Integer.parseInt(props.getProperty(PAR_ACTIVE_VIEW_SIZE, DEFAULT_ACTIVE_VIEW_SIZE));
        this.passiveViewSize = Integer.parseInt(props.getProperty(PAR_PASSIVE_VIEW_SIZE, DEFAULT_PASSIVE_VIEW_SIZE));
        this.ARWL = Integer.parseInt(props.getProperty(PAR_ARWL, DEFAULT_ARWL));
        this.PRWL = Integer.parseInt(props.getProperty(PAR_PRWL, DEFAULT_PRWL));

        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("babel.address"));
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("babel.port"));
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY,
                props.getProperty("channel_metrics_interval", "10000"));
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");

        channelId = createChannel(TCPChannel.NAME, channelProps);

        /* Serializers */
        registerMessageSerializer(channelId, JoinMessage.MSG_ID, JoinMessage.serializer);
        registerMessageSerializer(channelId, ForwardJoinMessage.MSG_ID, ForwardJoinMessage.serializer);
        registerMessageSerializer(channelId, DisconnectMessage.MSG_ID, DisconnectMessage.serializer);

        /* Message handlers */
        registerMessageHandler(channelId, JoinMessage.MSG_ID, this::uponJoin, this::uponMsgFail);
        registerMessageHandler(channelId, ForwardJoinMessage.MSG_ID, this::uponForwardJoin, this::uponMsgFail);
        registerMessageHandler(channelId, DisconnectMessage.MSG_ID, this::uponDisconnect, this::uponMsgFail);

        /* Channel event handlers */
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
    }

    @Override
    public void init(Properties props) {
        triggerNotification(new ChannelCreated(channelId));

        if (props.containsKey("contact")) {
            try {
                String[] hostElems = props.getProperty("contact").split(":");
                Host contactNode = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));

                if (!contactNode.equals(self)) {
                    openConnection(contactNode);
                    sendMessage(new JoinMessage(self), contactNode);
                }

            } catch (Exception e) {
                logger.error("Invalid contact: {}", props.getProperty("contact"));
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    /* -------------------------------------------------------------------------- */
    /* Messages */
    /* -------------------------------------------------------------------------- */

    private void uponJoin(JoinMessage msg, Host from, short sourceProto, int channelId) {
        Host newNode = msg.getNewNode();
        logger.info("Received {} from {}", msg, from);

        if (isActiveViewFull()) {
            dropRandomElementFromActiveView();
        }

        addNodeActiveView(newNode);

        for (Host n : new HashSet<>(activeView)) {
            if (!n.equals(newNode)) {
                sendMessage(new ForwardJoinMessage(newNode, ARWL, self), n);
            }
        }
    }

    private void uponForwardJoin(ForwardJoinMessage msg, Host from, short sourceProto, int channelId) {
        Host newNode = msg.getNewNode();
        int ttl = msg.getTimeToLive();
        Host sender = msg.getSender();

        logger.info("Received {} from {}", msg, from);

        if (newNode.equals(self))
            return;

        if (ttl == 0 || activeView.size() == 1) {
            addNodeActiveView(newNode);
        } else {
            if (ttl == PRWL) {
                addNodePassiveView(newNode);
            }

            Host n = getRandomNodeExcluding(activeView, sender);
            if (n != null) {
                sendMessage(new ForwardJoinMessage(newNode, ttl - 1, self), n);
            }
        }
    }

    private void uponDisconnect(DisconnectMessage msg, Host from, short sourceProto, int channelId) {
        Host peer = msg.getPeer();
        logger.info("Received {} from {}", msg, from);

        if (activeView.remove(peer)) {
            addNodePassiveView(peer);

            if (connectedPeers.remove(peer)) {
                triggerNotification(new NeighbourDown(peer));
            }
        }
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /* -------------------------------------------------------------------------- */
    /* Auxiliares */
    /* -------------------------------------------------------------------------- */

    private void dropRandomElementFromActiveView() {
        Host n = getRandomNode(activeView);
        if (n == null)
            return;

        sendMessage(new DisconnectMessage(self), n);

        activeView.remove(n);
        addNodePassiveView(n);

        if (connectedPeers.remove(n)) {
            triggerNotification(new NeighbourDown(n));
        }
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
            if (toRemove != null) {
                passiveView.remove(toRemove);
            }
        }

        passiveView.add(node);
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
    /* Channel events */
    /* -------------------------------------------------------------------------- */

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} is up", peer);

        if (activeView.contains(peer) && connectedPeers.add(peer)) {
            triggerNotification(new NeighbourUp(peer));
        }
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} is down cause {}", peer, event.getCause());

        if (activeView.remove(peer)) {
            addNodePassiveView(peer);

            if (connectedPeers.remove(peer)) {
                triggerNotification(new NeighbourDown(peer));
            }
        }
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} failed cause: {}", peer, event.getCause());

        if (activeView.remove(peer)) {
            addNodePassiveView(peer);

            if (connectedPeers.remove(peer)) {
                triggerNotification(new NeighbourDown(peer));
            }
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.info("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.info("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }
}