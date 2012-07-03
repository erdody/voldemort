package voldemort.secondary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import voldemort.TestUtils;
import voldemort.client.StoreClient;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.StorageEngine;
import voldemort.store.StorageEngine.KeyMatch;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/** Utility methods to test the secondary index support */
public class SecondaryIndexTestUtils {

    /** Provider for a byte array Store */
    public interface ByteArrayStorageEngineProvider {

        StorageEngine<ByteArray, byte[], byte[]> getStorageEngine();
    }

    /** Test value {@link SerializerDefinition} */
    public static final SerializerDefinition VALUE_SERIALIZER_DEF = new SerializerDefinition("json",
                                                                                             "{'data':'string', "
                                                                                                     + "'status':'int8', "
                                                                                                     + "'lastmod':'date', "
                                                                                                     + "'company':'string', "
                                                                                                     + "'feedless':'boolean'}");

    /** Test value {@link Serializer} */
    @SuppressWarnings("unchecked")
    public static final Serializer<Object> VALUE_SERIALIZER = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(VALUE_SERIALIZER_DEF);

    private static SecondaryIndexDefinition secIdx(String field, String type) {
        return new SecondaryIndexDefinition(field, "map", field, type);
    }

    /** Test list of {@link SecondaryIndexDefinition} */
    public static final List<SecondaryIndexDefinition> SEC_IDX_DEFS = ImmutableList.of(secIdx("status",
                                                                                              "int8"),
                                                                                       secIdx("lastmod",
                                                                                              "date"),
                                                                                       secIdx("company",
                                                                                              "string"),
                                                                                       secIdx("feedless",
                                                                                              "boolean"));

    /**
     * Test {@link SecondaryIndexProcessor}, based on {@link #SEC_IDX_DEFS} and
     * {@link #VALUE_SERIALIZER_DEF}
     */
    public static final SecondaryIndexProcessor SEC_IDX_PROCESSOR = SecondaryIndexProcessorFactory.getProcessor(new DefaultSerializerFactory(),
                                                                                                                SEC_IDX_DEFS,
                                                                                                                VALUE_SERIALIZER_DEF);

    private final ByteArrayStorageEngineProvider storeProvider;

    public SecondaryIndexTestUtils(ByteArrayStorageEngineProvider storeProvider) {
        this.storeProvider = storeProvider;
    }

    private StorageEngine<ByteArray, byte[], byte[]> getStorageEngine() {
        return storeProvider.getStorageEngine();
    }

    public static SecondaryIndexProcessor getSecIdxProcessor() {
        return SEC_IDX_PROCESSOR;
    }

    private ByteArray testKey(String key) {
        return new ByteArray(key.getBytes());
    }

    /** Creates a test value that complies with the serializer schema */
    public static Map<String, Object> createTestValue(String data,
                                                      int status,
                                                      Date lastMod,
                                                      String company,
                                                      Boolean feedless) {
        Map<String, Object> map = Maps.newHashMap();
        map.put("data", data);
        map.put("status", (byte) status);
        map.put("lastmod", lastMod);
        map.put("company", company);
        map.put("feedless", feedless);
        return map;
    }

    /**
     * Checks the given query return the given keys on the inner store. See
     * {@link StorageEngine#keys(String)}
     */
    public void assertQueryReturns(String query, String... expectedKeys) {
        ClosableIterator<KeyMatch<ByteArray>> keysIt = getStorageEngine().keys(query);
        try {
            List<String> result = Lists.newArrayList(Iterators.transform(keysIt,
                                                                         new Function<KeyMatch<ByteArray>, String>() {

                                                                             public String apply(KeyMatch<ByteArray> match) {
                                                                                 return new String(match.getKey()
                                                                                                        .get());
                                                                             }
                                                                         }));
            Collections.sort(result);

            assertEquals(Arrays.asList(expectedKeys), result);
        } finally {
            keysIt.close();
        }
    }

    public void assertQueryReturns(String query, List<KeyMatch<ByteArray>> expectedMatches) {
        ClosableIterator<KeyMatch<ByteArray>> keysIt = getStorageEngine().keys(query);
        try {
            assertEquals(Sets.newHashSet(expectedMatches), Sets.newHashSet(keysIt));
        } finally {
            keysIt.close();
        }
    }

    public static byte[] createTestSerializedValue(String data,
                                                   int status,
                                                   Date lastMod,
                                                   String company,
                                                   Boolean feedless) {
        return VALUE_SERIALIZER.toBytes(createTestValue(data, status, lastMod, company, feedless));
    }

    private Versioned<byte[]> putTestValue(String key,
                                           String data,
                                           int status,
                                           Date lastMod,
                                           String company,
                                           Boolean feedless) {
        List<Version> versions = getStorageEngine().getVersions(testKey(key));
        Version newVer = versions.isEmpty() ? new VectorClock()
                                           : ((VectorClock) Iterables.getLast(versions)).incremented(0,
                                                                                                     System.currentTimeMillis());
        return putTestValue(key, data, status, lastMod, company, feedless, newVer);
    }

    private Versioned<byte[]> putTestValue(String key,
                                           String data,
                                           int status,
                                           Date lastMod,
                                           String company,
                                           Boolean feedless,
                                           Version ver) {
        Versioned<byte[]> value = Versioned.value(createTestSerializedValue(data,
                                                                            status,
                                                                            lastMod,
                                                                            company,
                                                                            feedless),
                                                  ver);
        getStorageEngine().put(testKey(key), value, null);
        return value;
    }

    private void removeKey(String... keys) {
        for(String key: keys) {
            List<Versioned<byte[]>> values = getStorageEngine().get(testKey(key), null);
            assertFalse(values.isEmpty());
            getStorageEngine().delete(testKey(key), values.get(0).getVersion());
            assertTrue(getStorageEngine().get(testKey(key), null).isEmpty());
        }
    }

    /** Creates a byte array with keys k[from] .. k[to] (both inclusive). */
    private String[] keyRange(int from, int to) {
        String[] result = new String[to - from + 1];
        for(int i = 0; i < result.length; i++) {
            result[i] = "k" + (from + i);
        }
        return result;
    }

    private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

    private static String getFormattedDate(int pDays) {
        return df.format(days(pDays));
    }

    private static String lastModRange(int fromDays, int toDays) {
        return String.format("lastmod >= '%s' and lastmod <= '%s'",
                             getFormattedDate(fromDays),
                             getFormattedDate(toDays));
    }

    private static Date days(int days) {
        return new Date(TimeUnit.DAYS.toMillis(days));
    }

    /** Full fixture for secondary index testing, applied to the provided Store */
    public void testSecondaryIndex() throws Exception {
        putTestValue("k1", "myData1", 1, days(100), "c1", null);
        putTestValue("k2", "myData2", 2, days(101), "c1", true);
        putTestValue("k3", "myData3", 2, days(102), "c2", false);
        putTestValue("k4", "myData4", 1, days(103), "c3", false);

        putTestValue("k5", "myData5", 0, days(0), null, true);
        putTestValue("k6", "myData6", 6, days(150), "c2", true);
        putTestValue("k7", "myData7", 100, days(1500), "c1", null);

        assertQueryReturns("status = 1", "k1", "k4");
        assertQueryReturns("status = 2", "k2", "k3");
        assertQueryReturns("status in {1, 2}", keyRange(1, 4));
        assertQueryReturns("status >= 0 and status <= 100", keyRange(1, 7));

        assertQueryReturns(lastModRange(100, 100), "k1");
        assertQueryReturns(lastModRange(101, 102), "k2", "k3");
        assertQueryReturns(lastModRange(103, 105), "k4");
        assertQueryReturns(lastModRange(90, 110), keyRange(1, 4));
        assertQueryReturns(lastModRange(90, 95));
        assertQueryReturns(lastModRange(104, 105));

        assertQueryReturns("not feedless present", "k1", "k7");
        assertQueryReturns("feedless = 'true'", "k2", "k5", "k6");
        assertQueryReturns("not company present", "k5");
        assertQueryReturns("company ~ 'c[23]'", "k3", "k4", "k6");

        // update a value, and check it's properly re-indexed
        putTestValue("k1", "myData1", 3, days(115), "c1", true);
        assertQueryReturns(lastModRange(90, 110), keyRange(2, 4));
        assertQueryReturns(lastModRange(110, 140), "k1");
        assertQueryReturns("status in {1, 2}", keyRange(2, 4));
        assertQueryReturns("status = 3", "k1");
        assertQueryReturns("not feedless present", "k7");

        removeKey("k3");
        assertQueryReturns("status = 2", "k2");
        assertQueryReturns(lastModRange(101, 105), "k2", "k4");

        removeKey("k4");
        assertQueryReturns("status = 2", "k2");
        assertQueryReturns(lastModRange(101, 105), "k2");

        removeKey("k1", "k2");
        assertQueryReturns("status >= 0 and status <= 10", "k5", "k6");
        assertQueryReturns(lastModRange(0, 200), "k5", "k6");

        // now test what happens with multiple versions
        getStorageEngine().truncate();
        assertQueryReturns(lastModRange(0, 200));

        testMultiVersions();
    }

    private void testMultiVersions() {
        VectorClock vc = new VectorClock();
        // three concurrent versions, for testing
        VectorClock vc0 = vc.incremented(0, 0);
        VectorClock vc1 = vc.incremented(1, 1);
        VectorClock vc2 = vc.incremented(2, 2);

        putTestValue("k1", "myData1", 1, days(100), "c1", true, vc0);
        putTestValue("k1", "myData1", 2, days(101), "c1", true, vc1);
        putTestValue("k1", "myData1", 1, days(100), "c1", true, vc2);

        assertQueryReturns("status = 0");
        assertQueryReturns("status = 2",
                           Collections.singletonList(keyMatch("k1",
                                                              Arrays.asList(vc1),
                                                              Arrays.asList(vc0, vc2))));
        assertQueryReturns("status = 1",
                           Collections.singletonList(keyMatch("k1",
                                                              Arrays.asList(vc0, vc2),
                                                              Arrays.asList(vc1))));
        assertQueryReturns(lastModRange(0, 200),
                           Collections.singletonList(keyMatch("k1",
                                                              Arrays.asList(vc0, vc1, vc2),
                                                              Collections.<Version> emptyList())));

        // Now add a single version at the end and check things still work
        putTestValue("k2", "myData2", 3, days(102), "c1", true, vc);

        assertQueryReturns("status = 0");
        assertQueryReturns("status = 2",
                           Collections.singletonList(keyMatch("k1",
                                                              Arrays.asList(vc1),
                                                              Arrays.asList(vc0, vc2))));
        assertQueryReturns("status = 1",
                           Collections.singletonList(keyMatch("k1",
                                                              Arrays.asList(vc0, vc2),
                                                              Arrays.asList(vc1))));
        assertQueryReturns("status = 3", "k2");
        assertQueryReturns(lastModRange(0, 200), "k1", "k2");

        // And now add a new entry with two versions
        putTestValue("k3", "myData3", 4, days(100), "c1", true, vc0);
        putTestValue("k3", "myData3", 1, days(100), "c1", true, vc1);

        assertQueryReturns("status = 0");
        assertQueryReturns("status = 2",
                           Collections.singletonList(keyMatch("k1",
                                                              Arrays.asList(vc1),
                                                              Arrays.asList(vc0, vc2))));

        @SuppressWarnings("unchecked")
        List<KeyMatch<ByteArray>> matches = Arrays.asList(keyMatch("k1",
                                                                   Arrays.asList(vc0, vc2),
                                                                   Arrays.asList(vc1)),
                                                          keyMatch("k3",
                                                                   Arrays.asList(vc1),
                                                                   Arrays.asList(vc0)));
        assertQueryReturns("status = 1", matches);
        assertQueryReturns("status = 3", "k2");
        assertQueryReturns(lastModRange(0, 200), "k1", "k2", "k3");
    }

    private KeyMatch<ByteArray> keyMatch(String key,
                                         List<? extends Version> matchVersions,
                                         List<? extends Version> unmatchedVersions) {
        return new KeyMatch<ByteArray>(new ByteArray(key.getBytes()),
                                       matchVersions,
                                       unmatchedVersions);
    }

    public static List<byte[]> getValues(int numValues) {
        List<byte[]> values = Lists.newArrayList();
        for(int i = 0; i < numValues; i++) {
            values.add(getValue(10));
        }
        return values;
    }

    public static byte[] getValue(int size) {
        return createTestSerializedValue(new String(TestUtils.randomBytes(size)),
                                         (byte) TestUtils.SEEDED_RANDOM.nextInt() % 128,
                                         new Date(TestUtils.SEEDED_RANDOM.nextInt()),
                                         Integer.toString(TestUtils.SEEDED_RANDOM.nextInt()),
                                         TestUtils.SEEDED_RANDOM.nextBoolean());
    }

    /**
     * Fixture to test the end to end functionality of secondary index (put
     * values, query and check the results)
     */
    public static class FunctionalTest {

        private final String storeName;
        private final StoreClient<String, Map<String, ?>> client;
        private final AdminClient adminClient;

        public FunctionalTest(String storeName,
                              StoreClient<String, Map<String, ?>> client,
                              AdminClient adminClient) {
            this.storeName = storeName;
            this.client = client;
            this.adminClient = adminClient;
        }

        public void testSecondaryQuery() {
            client.put("k1", createTestValue("data1", 2, days(100), "c1", true));
            client.put("k2", createTestValue("data2", 1, days(101), "c2", false));
            client.put("k3", createTestValue("data3", 0, days(102), "c2", true));
            client.put("k4", createTestValue("data4", 3, days(103), "c1", false));

            assertQuery("status >= 1 and status <= 2", "k1", "k2");
            assertQuery("status = 1", "k2");
            assertQuery("status in {2, 3}", "k1", "k4");
            assertQuery(lastModRange(101, 102), "k2", "k3");

            assertQuery("company = 'c1'", "k1", "k4");
            assertQuery("company ~ '.2'", "k2", "k3"); // regex

            assertQuery("feedless = true", "k1", "k3");
            assertQuery("feedless = false or company = 'c2'", "k2", "k3", "k4");
        }

        private void assertQuery(String query, String... expectedKeys) {
            Iterator<ByteArray> fetchIt = adminClient.fetchKeysForQuery(0, storeName, query);

            // check values
            List<String> keys = Lists.newArrayList(Lists.transform(Lists.newArrayList(fetchIt),
                                                                   new Function<ByteArray, String>() {

                                                                       public String apply(ByteArray key) {
                                                                           return new String(key.get());
                                                                       }

                                                                   }));
            Collections.sort(keys);
            assertEquals(Arrays.asList(expectedKeys), keys);
        }

    }

}
