package leap.data.test.avro;

import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;
import kafka.utils.TestUtils;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import scala.Option;
import scala.Option$;
import scala.collection.JavaConversions;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

public class ConfluentTestHarness {
    public static final int DEFAULT_NUM_BROKERS = 1;
    public static final String KAFKASTORE_TOPIC = "_schemas";
    protected static final Option<Properties> EMPTY_SASL_PROPERTIES = Option$.MODULE$.empty();

    private final int numBrokers;
    private final boolean setupRestApp;
    protected String compatibilityType;

    // ZK Config
    protected EmbeddedZookeeper zookeeper;
    protected String zkConnect;
    protected ZkClient zkClient;
    protected ZkUtils zkUtils;
    protected int zkConnectionTimeout = 30000; // a larger connection timeout is required for SASL tests
    // because SASL connections tend to take longer.
    protected int zkSessionTimeout = 6000;

    // Kafka Config
    protected List<KafkaConfig> configs = null;
    protected List<KafkaServer> servers = null;
    protected String brokerList = null;
    private String bootstrapServers = null;

    protected int schemaRegistryPort;
    protected RestApp restApp = null;
    private String schemaRegistryUrl;


    public ConfluentTestHarness() {
        this(DEFAULT_NUM_BROKERS);
    }

    public ConfluentTestHarness(int numBrokers) {
        this(numBrokers, false);
    }

    public ConfluentTestHarness(int numBrokers, boolean setupRestApp) {
        this(numBrokers, setupRestApp, AvroCompatibilityLevel.NONE.name);
    }

    public ConfluentTestHarness(int numBrokers, boolean setupRestApp, String compatibilityType
    ) {
        this.numBrokers = numBrokers;
        this.setupRestApp = setupRestApp;
        this.compatibilityType = compatibilityType;
    }

    public void setUp() throws Exception {
        //System.setProperty("zookeeper.sasl.client", "false");
        zookeeper = new EmbeddedZookeeper();
        zkConnect = String.format("localhost:%d", zookeeper.port());
        zkUtils = ZkUtils.apply(
                zkConnect, zkSessionTimeout, zkConnectionTimeout,
                setZkAcls()
        ); // true or false doesn't matter because the schema registry Kafka principal is the same as the
        // Kafka broker principal, so ACLs won't make any difference. The principals are the same because
        // ZooKeeper, Kafka, and the Schema Registry are run in the same process during testing and hence share
        // the same JAAS configuration file. Read comments in ASLClusterTestHarness.java for more details.
        zkClient = zkUtils.zkClient();

        configs = new Vector<>();
        servers = new Vector<>();
        for (int i = 0; i < numBrokers; i++) {
            KafkaConfig config = getKafkaConfig(i);
            configs.add(config);

            KafkaServer server = TestUtils.createServer(config, Time.SYSTEM);
            servers.add(server);
        }

        brokerList =
                TestUtils.getBrokerListStrFromServers(
                        JavaConversions.asScalaBuffer(servers),
                        getSecurityProtocol()
                );

        // Initialize the rest app ourselves so we can ensure we don't pass any info about the Kafka
        // zookeeper. The format for this config includes the security protocol scheme in the URLs so
        // we can't use the pre-generated server list.
        String[] serverUrls = new String[servers.size()];
        ListenerName listenerType = ListenerName.forSecurityProtocol(getSecurityProtocol());
        for(int i = 0; i < servers.size(); i++) {
            serverUrls[i] = getSecurityProtocol() + "://" +
                    Utils.formatAddress(
                            servers.get(i).config().advertisedListeners().head().host(),
                            servers.get(i).boundPort(listenerType)
                    );
        }
        bootstrapServers = Utils.join(serverUrls, ",");

        if (setupRestApp) {
            schemaRegistryPort = choosePort();
            Properties schemaRegistryProps = getSchemaRegistryProperties();
            schemaRegistryProps.put("listeners", getSchemaRegistryProtocol() +
                    "://0.0.0.0:"
                    + schemaRegistryPort);
            schemaRegistryProps.put("mode.mutability", true);
            schemaRegistryUrl = schemaRegistryProps.getProperty("listeners");
            setupRestApp(schemaRegistryProps);

        }
    }

    public void tearDown() throws Exception {
        if (restApp != null) {
            restApp.stop();
        }

        if (servers != null) {
            for (KafkaServer server : servers) {
                server.shutdown();
            }

            // Remove any persistent data
            for (KafkaServer server : servers) {
                CoreUtils.delete(server.config().logDirs());
            }
        }

        if (zkUtils != null) {
            zkUtils.close();
        }

        if (zookeeper != null) {
            zookeeper.shutdown();
        }
    }

    private boolean setZkAcls() {
        return getSecurityProtocol() == SecurityProtocol.SASL_PLAINTEXT ||
                getSecurityProtocol() == SecurityProtocol.SASL_SSL;
    }

    private void setupRestApp(Properties schemaRegistryProps) throws Exception {
        restApp = new RestApp(schemaRegistryPort, zkConnect, null, KAFKASTORE_TOPIC,
                compatibilityType, true, schemaRegistryProps);
        restApp.start();
    }

    private static int choosePort() {
        return choosePorts(1)[0];
    }

    private static int[] choosePorts(int count) {
        try {
            ServerSocket[] sockets = new ServerSocket[count];
            int[] ports = new int[count];
            for (int i = 0; i < count; i++) {
                sockets[i] = new ServerSocket(0, 0, InetAddress.getByName("0.0.0.0"));
                ports[i] = sockets[i].getLocalPort();
            }
            for (int i = 0; i < count; i++) {
                sockets[i].close();
            }
            return ports;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private SecurityProtocol getSecurityProtocol() {
        return SecurityProtocol.PLAINTEXT;
    }

    private String getSchemaRegistryProtocol() {
        return "http";
    }

    private Properties getSchemaRegistryProperties() {
        return new Properties();
    }

    public KafkaConfig getKafkaConfig(int brokerId) {

        final Option<File> noFile = scala.Option.apply(null);
        final Option<SecurityProtocol> noInterBrokerSecurityProtocol = scala.Option.apply(null);
        Properties props = TestUtils.createBrokerConfig(
                brokerId,
                zkConnect,
                false,
                false,
                TestUtils.RandomPort(),
                noInterBrokerSecurityProtocol,
                noFile,
                EMPTY_SASL_PROPERTIES,
                true,
                false,
                TestUtils.RandomPort(),
                false,
                TestUtils.RandomPort(),
                false,
                TestUtils.RandomPort(),
                Option.empty(),
                1,
                false
        );
        injectProperties(props);
        return KafkaConfig.fromProps(props);

    }

    private void injectProperties(Properties props) {
        props.setProperty("auto.create.topics.enable", "true");
        props.setProperty("num.partitions", "1");
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public void setSchemaRegistryUrl(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }
}
