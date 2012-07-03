package voldemort.server.secondary;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.secondary.SecondaryIndexTestUtils;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

import com.google.common.collect.Lists;

/**
 * Provides an unmocked end to end unit test of a Voldemort cluster, with BDB
 * and secondary index enabled.
 */
@RunWith(Parameterized.class)
public class SecondaryEndToEndTest {

    private static final String STORE_NAME = "test-secondary-index";
    private static final String STORE_NAME_WITH_COMPRESSION = "test-secondary-index-with-compression";
    private static final String STORES_XML = "test/common/voldemort/config/stores-secondary-index.xml";
    private final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                        10000,
                                                                                        100000,
                                                                                        32 * 1024);
    private AdminClient adminClient;

    private final boolean useNio;

    private StoreClientFactory storeClientFactory;

    public SecondaryEndToEndTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Before
    public void setUp() throws IOException {
        final int nodes = 3;
        Cluster cluster = ServerTestUtils.getLocalCluster(nodes, new int[][] { { 0, 3, 6, 9 },
                { 1, 4, 7, 10 }, { 2, 5, 8, 11 } });
        List<VoldemortServer> servers = Lists.newArrayList();
        for(int i = 0; i < nodes; i++) {
            servers.add(ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                             ServerTestUtils.createServerConfig(useNio,
                                                                                                i,
                                                                                                TestUtils.createTempDir()
                                                                                                         .getAbsolutePath(),
                                                                                                null,
                                                                                                STORES_XML,
                                                                                                new Properties()),
                                                             cluster));
        }
        Node node = cluster.getNodeById(0);
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        storeClientFactory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
        adminClient = ServerTestUtils.getAdminClient(cluster);
    }

    @After
    public void tearDown() {
        socketStoreFactory.close();
        adminClient.stop();
    }

    private StoreClient<String, Map<String, ?>> getStoreClient(String storeName) {
        return storeClientFactory.getStoreClient(storeName);
    }

    /**
     * Test the secondary index functionality
     */
    @Test
    public void testSanity() {
        new SecondaryIndexTestUtils.FunctionalTest(STORE_NAME,
                                                   getStoreClient(STORE_NAME),
                                                   adminClient).testSecondaryQuery();
    }

    /**
     * Test the secondary index functionality with value compression enabled
     */
    @Test
    public void testSanityWithCompression() {
        new SecondaryIndexTestUtils.FunctionalTest(STORE_NAME_WITH_COMPRESSION,
                                                   getStoreClient(STORE_NAME_WITH_COMPRESSION),
                                                   adminClient).testSecondaryQuery();
    }

}
