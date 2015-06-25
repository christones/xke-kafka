package fr.xebia.kafka;

import fr.xebia.kafka.solutions.producer.XNewKafkaProducer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerTest {

    private KafkaServer kafkaServer;
    private ZkClient zkClient;
    private EmbeddedZookeeper zkServer;

    private int port;
    private int brokerId = 0;
    private String topic = "test";

    @Before
    public void init() {
        // setup Zookeeper
        String zkConnect = TestZKUtils.zookeeperConnect();
        zkServer = new EmbeddedZookeeper(zkConnect);
        zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        // setup Broker
        port = TestUtils.choosePort();
        Properties props = TestUtils.createBrokerConfig(brokerId, port, false);

        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        List<KafkaServer> kafkaServers = new ArrayList<KafkaServer>();
        kafkaServers.add(kafkaServer);
        Buffer<KafkaServer> servers = JavaConversions.asScalaBuffer(kafkaServers);

        // create topic
        TestUtils.createTopic(zkClient, topic, 1, 1, servers, new Properties());
        TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0, 5000);
    }

    @Test
    public void producerTest() throws InterruptedException, ExecutionException {
        XNewKafkaProducer XNewKafkaProducer = new XNewKafkaProducer(topic);

        XNewKafkaProducer.configure("localhost:" + port, "async");

        XNewKafkaProducer.start();

        XNewKafkaProducer.produce("TEST");

        XNewKafkaProducer.close();
    }

    @After
    public void close() {
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }

}

