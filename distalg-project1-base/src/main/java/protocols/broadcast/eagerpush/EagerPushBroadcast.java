package protocols.broadcast.eagerpush;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.broadcast.eagerpush.messages.EagerPushMessage;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class EagerPushBroadcast extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(EagerPushBroadcast.class);

    public static final String PROTOCOL_NAME = "EagerPush";
    public static final short PROTOCOL_ID = 400;

    public static final String PAR_FANOUT = "protocol.broadcast.eagerpush.fanout";
    public static final String PAR_DEFAULT_FANOUT = "2";

    private final Host myself;
    private final Set<Host> neighbours;
    private final Set<UUID> received;

    private final int fanout;
    private final Random rnd;

    private boolean channelReady;

    public EagerPushBroadcast(Properties properties, Host myself) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.myself = myself;
        this.neighbours = new HashSet<>();
        this.received = new HashSet<>();
        this.channelReady = false;
        this.fanout = Integer.parseInt(properties.getProperty(PAR_FANOUT, PAR_DEFAULT_FANOUT));
        this.rnd = new Random();

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
    }

    @Override
    public void init(Properties props) {
        // Nothing to do
    }

    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
        int cId = notification.getChannelId();

        registerSharedChannel(cId);

        registerMessageSerializer(cId, EagerPushMessage.MSG_ID, EagerPushMessage.serializer);

        try {
            registerMessageHandler(cId, EagerPushMessage.MSG_ID, this::uponEagerPushMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering eager-push handler: {}", e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        channelReady = true;
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
        if (!channelReady)
            return;

        EagerPushMessage msg = new EagerPushMessage(
                request.getMsgId(),
                request.getSender(),
                sourceProto,
                request.getMsg());

        uponEagerPushMessage(msg, myself, getProtoId(), -1);
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponEagerPushMessage(EagerPushMessage msg, Host from, short sourceProto, int channelId) {
        logger.trace("Received {} from {}", msg, from);

        // First time only
        if (received.add(msg.getMid())) {
            logger.info("FIRST TIME {} → will deliver and forward", msg.getMid());

            logger.info("DELIVER {} to application", msg.getMid());
            triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent()));

            Set<Host> targets = getRandomSubsetExcluding(neighbours, fanout, from);

            logger.info("FORWARD {} to {} neighbours: {}", msg.getMid(), targets.size(), targets);

            for (Host h : targets) {
                logger.info("Sending {} to {}", msg.getMid(), h);
                sendMessage(msg, h);
            }
        } else {
            logger.info("DUPLICATE {} from {} → ignored", msg.getMid(), from);
        }
    }

    private Set<Host> getRandomSubsetExcluding(Set<Host> hostSet, int sampleSize, Host exclude) {
        List<Host> list = new ArrayList<>(hostSet);
        list.remove(exclude);
        Collections.shuffle(list, rnd);
        return new HashSet<>(list.subList(0, Math.min(sampleSize, list.size())));
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /*--------------------------------- Notifications ---------------------------------------- */
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
}
