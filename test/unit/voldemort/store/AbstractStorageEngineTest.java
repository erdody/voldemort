/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store;

import static voldemort.TestUtils.getClock;

import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import voldemort.TestUtils;
import voldemort.secondary.SecondaryIndexTestUtils;
import voldemort.secondary.SecondaryIndexTestUtils.ByteArrayStorageEngineProvider;
import voldemort.serialization.StringSerializer;
import voldemort.store.serialized.SerializingStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public abstract class AbstractStorageEngineTest extends AbstractByteArrayStoreTest implements
        ByteArrayStorageEngineProvider {

    protected SecondaryIndexTestUtils secIdxTestUtils = new SecondaryIndexTestUtils(this);

    public void testGetNoEntries() {
        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> it = null;
        try {
            StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();
            it = engine.entries();
            while(it.hasNext())
                fail("There shouldn't be any entries in this store.");
        } finally {
            if(it != null)
                it.close();
        }
    }

    public void testGetNoKeys() {
        ClosableIterator<ByteArray> it = null;
        try {
            StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();
            it = engine.keys();
            while(it.hasNext())
                fail("There shouldn't be any entries in this store.");
        } finally {
            if(it != null)
                it.close();
        }
    }

    private StorageEngine<String, Object, String> getSerializingStorageEngine() {
        return SerializingStorageEngine.wrap(getStorageEngine(),
                                             new StringSerializer(),
                                             SecondaryIndexTestUtils.VALUE_SERIALIZER,
                                             new StringSerializer());
    }

    private Map<String, Object> getSerializingValues() {
        Map<String, Object> vals = Maps.newHashMap();
        for(String key: ImmutableList.of("a", "b", "c", "d")) {
            vals.put(key,
                     SecondaryIndexTestUtils.createTestValue(key, 1, new Date(), "company", true));
        }
        return vals;
    }

    public void testKeyIterationWithSerialization() {
        StorageEngine<String, Object, String> stringStore = getSerializingStorageEngine();
        Map<String, Object> vals = getSerializingValues();

        for(Map.Entry<String, Object> entry: vals.entrySet()) {
            stringStore.put(entry.getKey(), new Versioned<Object>(entry.getValue()), null);
        }

        List<String> expected = Lists.newArrayList(vals.keySet());
        Collections.sort(expected);

        ClosableIterator<String> iter = stringStore.keys();
        List<String> returnedKeys = Lists.newArrayList(iter);
        iter.close();
        Collections.sort(returnedKeys);

        assertEquals(expected, returnedKeys);
    }

    public void testIterationWithSerialization() {
        StorageEngine<String, Object, String> stringStore = getSerializingStorageEngine();
        Map<String, Object> vals = getSerializingValues();

        for(Map.Entry<String, Object> entry: vals.entrySet())
            stringStore.put(entry.getKey(), new Versioned<Object>(entry.getValue()), null);
        ClosableIterator<Pair<String, Versioned<Object>>> iter = stringStore.entries();
        int count = 0;
        while(iter.hasNext()) {
            Pair<String, Versioned<Object>> keyAndVal = iter.next();
            assertTrue(vals.containsKey(keyAndVal.getFirst()));
            assertEquals(vals.get(keyAndVal.getFirst()), keyAndVal.getSecond().getValue());
            count++;
        }
        assertEquals(count, vals.size());
        iter.close();
    }

    public void testPruneOnWrite() {
        StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();

        List<byte[]> values = getValues(3);
        Versioned<byte[]> v1 = new Versioned<byte[]>(values.get(0), TestUtils.getClock(1));
        Versioned<byte[]> v2 = new Versioned<byte[]>(values.get(1), TestUtils.getClock(2));
        Versioned<byte[]> v3 = new Versioned<byte[]>(values.get(2), TestUtils.getClock(1, 2));
        ByteArray key = getKey();
        engine.put(key, v1, null);
        engine.put(key, v2, null);
        assertEquals(2, engine.get(key, null).size());
        engine.put(key, v3, null);
        assertEquals(1, engine.get(key, null).size());
    }

    public void testTruncate() throws Exception {
        StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();

        int numEntries = 3;
        List<byte[]> values = getValues(numEntries);
        List<ByteArray> keys = getKeys(numEntries);
        for(int i = 0; i < numEntries; i++) {
            engine.put(keys.get(i), new Versioned<byte[]>(values.get(i)), null);
        }

        engine.truncate();

        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> it = null;
        try {
            it = engine.entries();
            while(it.hasNext()) {
                fail("There shouldn't be any entries in this store.");
            }
        } finally {
            if(it != null) {
                it.close();
            }
        }
    }

    /**
     * Check entries() work as expected with multiple versions
     */
    @Test
    public void testEntries() throws Exception {
        ByteArray key = getKey();
        StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();

        VectorClock c1 = getClock(1, 1);
        VectorClock c2 = getClock(1, 2);
        byte[] value = getValue();

        assertEntries(0, engine.entries(), engine.keys());

        // put two conflicting versions for the same key
        engine.put(key, new Versioned<byte[]>(value, c1), null);
        engine.put(key, new Versioned<byte[]>(value, c2), null);

        assertEntries(2, engine.entries(), engine.keys());

        engine.put(getKey(), new Versioned<byte[]>(value, c2), null);
        assertEntries(3, engine.entries(), engine.keys());
    }

    private void assertEntries(int expected, ClosableIterator... its) {
        try {
            for(ClosableIterator it: its)
                assertEquals(expected, Iterators.size(it));
        } finally {
            for(ClosableIterator it: its)
                it.close();
        }
    }

    @SuppressWarnings("unused")
    private boolean remove(List<byte[]> list, byte[] item) {
        Iterator<byte[]> it = list.iterator();
        boolean removedSomething = false;
        while(it.hasNext()) {
            if(TestUtils.bytesEqual(item, it.next())) {
                it.remove();
                removedSomething = true;
            }
        }
        return removedSomething;
    }

    @Test
    public void testSecondaryIndex() throws Exception {
        if(!isSecondaryIndexEnabled())
            return;
        secIdxTestUtils.testSecondaryIndex();
    }

    @Override
    public Store<ByteArray, byte[], byte[]> getStore() {
        return getStorageEngine();
    }

}
