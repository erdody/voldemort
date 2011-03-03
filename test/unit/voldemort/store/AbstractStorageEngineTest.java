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

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import voldemort.TestUtils;
import voldemort.secondary.SecondaryIndexTestUtils;
import voldemort.serialization.StringSerializer;
import voldemort.store.serialized.SerializingStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public abstract class AbstractStorageEngineTest extends AbstractByteArrayStoreTest {

    @Override
    public Store<ByteArray, byte[], byte[]> getStore() {
        return getStorageEngine();
    }

    public abstract StorageEngine<ByteArray, byte[], byte[]> getStorageEngine();

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
            vals.put(key, SecondaryIndexTestUtils.testValue(key, 1, new Date()));
        }
        return vals;
    }

    public void testKeyIterationWithSerialization() {
        StorageEngine<String, Object, String> stringStore = getSerializingStorageEngine();
        Map<String, Object> vals = getSerializingValues();

        for(Map.Entry<String, Object> entry: vals.entrySet())
            stringStore.put(entry.getKey(), new Versioned<Object>(entry.getValue()), null);
        ClosableIterator<String> iter = stringStore.keys();
        int count = 0;
        while(iter.hasNext()) {
            String key = iter.next();
            assertTrue(vals.containsKey(key));
            count++;
        }
        assertEquals(count, vals.size());
        iter.close();
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

}
