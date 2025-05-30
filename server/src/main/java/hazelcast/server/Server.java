package hazelcast.server;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
public class Server {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {
        logger.info(" Server Starting ...");

        Config config = new Config();

        GroupConfig groupConfig = new GroupConfig().setName("l12345").setPassword("l12345-pass");
        config.setGroupConfig(groupConfig);

        MulticastConfig multicastConfig = new MulticastConfig();

        JoinConfig joinConfig = new JoinConfig().setMulticastConfig(multicastConfig);

        InterfacesConfig interfacesConfig = new InterfacesConfig()
                .setInterfaces(Collections.singletonList("127.0.0.*")).setEnabled(true);

        NetworkConfig networkConfig = new NetworkConfig().setInterfaces(interfacesConfig).setJoin(joinConfig);

        config.setNetworkConfig(networkConfig);

        Hazelcast.newHazelcastInstance(config);
    }
}
