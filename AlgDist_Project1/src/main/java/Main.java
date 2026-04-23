import java.net.InetAddress;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.apps.BroadcastApp;
import protocols.broadcast.eagerpush.EagerPushBroadcast;
import protocols.broadcast.flood.FloodBroadcast;
import protocols.broadcast.hybrid.HybridGossipBroadcast;
import protocols.membership.cyclon.CyclonMembership;
import protocols.membership.full.GossipBasedFullMembership;
import protocols.membership.hyparview.HyParViewMembership;
import pt.unl.fct.di.novasys.babel.core.Babel;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.InterfaceToIp;

public class Main {

    static {
        System.setProperty("log4j.configurationFile", "log4j2.xml");
    }

    private static final Logger logger = LogManager.getLogger(Main.class);

    private static final String DEFAULT_CONF = "babel_config.properties";

    private static final String PAR_BROADCAST_PROTOCOL = "broadcast.protocol";
    private static final String PAR_MEMBERSHIP_PROTOCOL = "membership.protocol";

    private static final String DEFAULT_BROADCAST_PROTOCOL = "flood";
    private static final String DEFAULT_MEMBERSHIP_PROTOCOL = "full";

    public static void main(String[] args) throws Exception {

        Babel babel = Babel.getInstance();

        Properties props = Babel.loadConfig(args, DEFAULT_CONF);

        InterfaceToIp.addInterfaceIp(props);

        Host myself = new Host(
                InetAddress.getByName(props.getProperty("babel.address")),
                Integer.parseInt(props.getProperty("babel.port")));

        logger.info("Hello, I am {}", myself);

        String broadcastChoice = props.getProperty(PAR_BROADCAST_PROTOCOL, DEFAULT_BROADCAST_PROTOCOL).trim()
                .toLowerCase();
        String membershipChoice = props.getProperty(PAR_MEMBERSHIP_PROTOCOL, DEFAULT_MEMBERSHIP_PROTOCOL).trim()
                .toLowerCase();

        GenericProtocol membership = createMembership(membershipChoice, props, myself);
        GenericProtocol broadcast = createBroadcast(broadcastChoice, props, myself);

        short broadcastProtoId = getBroadcastProtoId(broadcastChoice);
        BroadcastApp broadcastApp = new BroadcastApp(myself, props, broadcastProtoId);

        babel.registerProtocol(broadcastApp);
        babel.registerProtocol(broadcast);
        babel.registerProtocol(membership);

        broadcastApp.init(props);
        broadcast.init(props);
        membership.init(props);

        babel.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> logger.info("Goodbye")));
    }

    private static GenericProtocol createMembership(String membershipChoice, Properties props, Host myself)
            throws Exception {
        switch (membershipChoice) {
            case "full":
                return new GossipBasedFullMembership(props, myself);
            case "cyclon":
                return new CyclonMembership(props, myself);
            case "hyparview":
                return new HyParViewMembership(props, myself);
            default:
                throw new IllegalArgumentException("Unknown membership.protocol: " + membershipChoice);
        }
    }

    private static GenericProtocol createBroadcast(String broadcastChoice, Properties props, Host myself)
            throws Exception {
        switch (broadcastChoice) {
            case "flood":
                return new FloodBroadcast(props, myself);
            case "eagerpush":
                return new EagerPushBroadcast(props, myself);
            case "hybrid":
                return new HybridGossipBroadcast(props, myself);
            default:
                throw new IllegalArgumentException("Unknown broadcast.protocol: " + broadcastChoice);
        }
    }

    private static short getBroadcastProtoId(String broadcastChoice) {
        switch (broadcastChoice) {
            case "flood":
                return FloodBroadcast.PROTOCOL_ID;
            case "eagerpush":
                return EagerPushBroadcast.PROTOCOL_ID;
            case "hybrid":
                return HybridGossipBroadcast.PROTOCOL_ID;
            default:
                throw new IllegalArgumentException("Unknown broadcast.protocol: " + broadcastChoice);
        }
    }
}
