package protocols.broadcast.hybrid;

import java.io.IOException;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.broadcast.hybrid.messages.GossipFullMessage;
import protocols.broadcast.hybrid.messages.IHaveMessage;
import protocols.broadcast.hybrid.messages.RequestMessage;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class HybridGossipBroadcast extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(HybridGossipBroadcast.class);

    public static final String PROTOCOL_NAME = "HybridGossipBroadcast";
    public static final short PROTOCOL_ID = 450;

    public static final String PAR_EAGER_FANOUT = "protocol.broadcast.hybrid.eager_fanout";
    public static final String PAR_LAZY_FANOUT = "protocol.broadcast.hybrid.lazy_fanout";
    public static final String PAR_MIN_EAGER_FANOUT = "protocol.broadcast.hybrid.min_eager_fanout";
    public static final String PAR_MAX_EAGER_FANOUT = "protocol.broadcast.hybrid.max_eager_fanout";
    public static final String PAR_ADAPT_WINDOW = "protocol.broadcast.hybrid.adapt_window";

    public static final String DEFAULT_EAGER_FANOUT = "2";
    public static final String DEFAULT_LAZY_FANOUT = "2";
    public static final String DEFAULT_MIN_EAGER_FANOUT = "1";
    public static final String DEFAULT_MAX_EAGER_FANOUT = "4";
    public static final String DEFAULT_ADAPT_WINDOW = "20";

    private final Host myself;
    private final Set<Host> neighbours;

    private final Set<UUID> received;
    private final Map<UUID, GossipFullMessage> messageCache;
    private final Set<UUID> requested;

    private final Random rnd;

    private final int eagerFanout;
    private final int lazyFanout;
    private final int minEagerFanout;
    private final int maxEagerFanout;
    private final int adaptWindow;

    private int currentEagerFanout;

    private int duplicateCount;
    private int newCount;

    private boolean channelReady;

    public HybridGossipBroadcast(Properties props, Host myself) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.myself = myself;
        this.neighbours = new HashSet<>();
        this.received = new HashSet<>();
        this.messageCache = new HashMap<>();
        this.requested = new HashSet<>();
        this.rnd = new Random();
        this.channelReady = false;

        this.eagerFanout = Integer.parseInt(props.getProperty(PAR_EAGER_FANOUT, DEFAULT_EAGER_FANOUT));
        this.lazyFanout = Integer.parseInt(props.getProperty(PAR_LAZY_FANOUT, DEFAULT_LAZY_FANOUT));
        this.minEagerFanout = Integer.parseInt(props.getProperty(PAR_MIN_EAGER_FANOUT, DEFAULT_MIN_EAGER_FANOUT));
        this.maxEagerFanout = Integer.parseInt(props.getProperty(PAR_MAX_EAGER_FANOUT, DEFAULT_MAX_EAGER_FANOUT));
        this.adaptWindow = Integer.parseInt(props.getProperty(PAR_ADAPT_WINDOW, DEFAULT_ADAPT_WINDOW));

        this.currentEagerFanout = this.eagerFanout;
        this.duplicateCount = 0;
        this.newCount = 0;

        /*--------------------- Request handlers -----------------------------*/
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

        /*--------------------- Notification handlers -----------------------------*/
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
    }

    @Override
    public void init(Properties props) {
    }

    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
        int cId = notification.getChannelId();
        registerSharedChannel(cId);

        registerMessageSerializer(cId, GossipFullMessage.MSG_ID, GossipFullMessage.serializer);
        registerMessageSerializer(cId, IHaveMessage.MSG_ID, IHaveMessage.serializer);
        registerMessageSerializer(cId, RequestMessage.MSG_ID, RequestMessage.serializer);

        try {
            registerMessageHandler(cId, GossipFullMessage.MSG_ID, this::uponGossipFullMessage, this::uponMsgFail);
            registerMessageHandler(cId, IHaveMessage.MSG_ID, this::uponIHaveMessage, this::uponMsgFail);
            registerMessageHandler(cId, RequestMessage.MSG_ID, this::uponRequestMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering handlers: {}", e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        channelReady = true;
    }

    /*------------------------------------------------------------------------*/
    /* Requests */
    /*------------------------------------------------------------------------*/

    private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
        if (!channelReady)
            return;

        GossipFullMessage msg = new GossipFullMessage(
                request.getMsgId(),
                request.getSender(),
                sourceProto,
                request.getMsg());

        uponGossipFullMessage(msg, myself, getProtoId(), -1);
    }

    /*------------------------------------------------------------------------*/
    /* Messages */
    /*------------------------------------------------------------------------*/

    private void uponGossipFullMessage(GossipFullMessage msg, Host from, short sourceProto, int channelId) {
        logger.trace("Received FULL {} from {}", msg, from);

        UUID mid = msg.getMid();

        if (received.add(mid)) {
            newCount++;

            logger.info("FIRST TIME {} from {} | eagerFanout={}", mid, from, currentEagerFanout);

            messageCache.put(mid, msg);

            triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent()));

            Set<Host> eagerTargets = getRandomSubsetExcluding(neighbours, currentEagerFanout, from);
            Set<Host> lazyTargets = getRandomSubsetExcluding(neighbours, lazyFanout, from, eagerTargets);

            logger.info("EAGER targets for {} -> {}", mid, eagerTargets);
            logger.info("LAZY targets for {} -> {}", mid, lazyTargets);

            for (Host h : eagerTargets) {
                sendMessage(msg, h);
            }

            IHaveMessage ihave = new IHaveMessage(mid, myself);
            for (Host h : lazyTargets) {
                sendMessage(ihave, h);
            }

        } else {
            duplicateCount++;
            logger.info("DUPLICATE FULL {} from {}", mid, from);
        }

        adaptFanoutIfNeeded();
    }

    private void uponIHaveMessage(IHaveMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received IHAVE {} from {}", msg, from);

        UUID mid = msg.getMid();

        if (received.contains(mid)) {
            logger.trace("Already have {}, ignoring IHAVE from {}", mid, from);
            return;
        }

        if (requested.add(mid)) {
            logger.info("REQUESTING missing {} from {}", mid, from);
            sendMessage(new RequestMessage(mid, myself), from);
        }
    }

    private void uponRequestMessage(RequestMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received REQUEST {} from {}", msg, from);

        GossipFullMessage full = messageCache.get(msg.getMid());

        if (full != null) {
            logger.info("REPLYING FULL {} to {}", msg.getMid(), from);
            sendMessage(full, from);
        } else {
            logger.info("REQUEST for {} but not in cache", msg.getMid());
        }
    }

    private void adaptFanoutIfNeeded() {
        int total = newCount + duplicateCount;
        if (total < adaptWindow)
            return;

        double dupRate = (double) duplicateCount / total;
        int oldFanout = currentEagerFanout;

        if (dupRate > 0.50) {
            currentEagerFanout = Math.max(minEagerFanout, currentEagerFanout - 1);
        } else if (dupRate < 0.20) {
            currentEagerFanout = Math.min(maxEagerFanout, currentEagerFanout + 1);
        }

        logger.info("ADAPT fanout: old={} new={} dupRate={} newMsgs={} dupMsgs={}",
                oldFanout, currentEagerFanout, String.format("%.2f", dupRate), newCount, duplicateCount);

        newCount = 0;
        duplicateCount = 0;
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /*------------------------------------------------------------------------*/
    /* Membership notifications */
    /*------------------------------------------------------------------------*/

    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        for (Host h : notification.getNeighbours()) {
            neighbours.add(h);
            logger.info("New neighbour: {}", h);
        }
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        for (Host h : notification.getNeighbours()) {
            neighbours.remove(h);
            logger.info("Neighbour down: {}", h);
        }
    }

    /*------------------------------------------------------------------------*/
    /* Auxiliares */
    /*------------------------------------------------------------------------*/

    private Set<Host> getRandomSubsetExcluding(Set<Host> hostSet, int sampleSize, Host exclude) {
        List<Host> list = new ArrayList<>(hostSet);
        list.remove(exclude);
        Collections.shuffle(list, rnd);

        return new HashSet<>(list.subList(0, Math.min(sampleSize, list.size())));
    }

    private Set<Host> getRandomSubsetExcluding(Set<Host> hostSet, int sampleSize, Host exclude, Set<Host> alsoExclude) {
        List<Host> list = new ArrayList<>();

        for (Host h : hostSet) {
            if (!h.equals(exclude) && !alsoExclude.contains(h)) {
                list.add(h);
            }
        }

        Collections.shuffle(list, rnd);
        return new HashSet<>(list.subList(0, Math.min(sampleSize, list.size())));
    }
}