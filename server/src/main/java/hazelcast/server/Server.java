package hazelcast.server;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class Server {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private static final String GROUP_NAME = "g12";
    private static final String GROUP_PASSWORD = "g12-pass";
    private static String interfaces;
    private static String port;

    public static void main(String[] args) {
        logger.info(" Server Starting ...");

        interfaces = System.getProperty("interface");

        if (interfaces == null || interfaces.isEmpty()) {
            logger.info("Missing Dinterface argument, using default 127.0.0.*");
            interfaces = "127.0.0.*";
        }

        port = System.getProperty("port");

        Config config = new Config();

        GroupConfig groupConfig = new GroupConfig().setName(GROUP_NAME).setPassword(GROUP_PASSWORD);
        config.setGroupConfig(groupConfig);

        MulticastConfig multicastConfig = new MulticastConfig();

        JoinConfig joinConfig = new JoinConfig().setMulticastConfig(multicastConfig);

        InterfacesConfig interfacesConfig = new InterfacesConfig()
                .setInterfaces(Collections.singletonList(interfaces))
                .setEnabled(true);

        NetworkConfig networkConfig = new NetworkConfig().setInterfaces(interfacesConfig);
        if (port == null || port.isEmpty()) {
            logger.info("Missing Dport argument, using default.");
        } else {
            if (!port.matches("\\d+")) {
                throw new IllegalArgumentException("Invalid port number: " + port);
            }
            logger.info("Using port: " + port);
            networkConfig.setPort(Integer.valueOf(port));
        }
        networkConfig.setJoin(joinConfig);

        config.setNetworkConfig(networkConfig);

        Hazelcast.newHazelcastInstance(config);
    }
}
