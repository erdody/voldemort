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

package voldemort.scheduled;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Semaphore;

import junit.framework.TestCase;
import voldemort.MockTime;
import voldemort.secondary.SecondaryIndexTestUtils;
import voldemort.server.scheduler.DataCleanupJob;
import voldemort.store.StorageEngine;
import voldemort.store.memory.InMemoryStorageEngineSI;
import voldemort.utils.ByteArray;
import voldemort.utils.DefaultIterable;
import voldemort.utils.EventThrottler;
import voldemort.utils.Time;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class DataCleanupJobTest extends TestCase {

    private MockTime time;
    private StorageEngine<ByteArray, byte[], byte[]> engine;

    @Override
    public void setUp() {
        time = new MockTime();
        engine = new InMemoryStorageEngineSI("test", SecondaryIndexTestUtils.SEC_IDX_PROCESSOR);
    }

    public void testCleanupCleansUp() {
        put("a", "b", "c");

        time.addMilliseconds(Time.MS_PER_DAY + 1);
        put("d", "e", "f");
        assertContains("a", "b", "c", "d", "e", "f");

        // update a single item to bump its vector clock time
        put("a");

        // now run cleanup
        new DataCleanupJob<ByteArray, byte[], byte[]>(engine,
                                                      new Semaphore(1),
                                                      Time.MS_PER_DAY,
                                                      time,
                                                      new EventThrottler(1),
                                                      null).run();

        // Check that all the later keys are there AND the key updated later
        assertContains("a", "d", "e", "f");
    }

    private void put(String... items) {
        for(String item: items) {
            ByteArray key = serializeKey(item);
            VectorClock clock = null;
            List<Versioned<byte[]>> found = engine.get(key, null);
            if(found.size() == 0) {
                clock = new VectorClock(time.getMilliseconds());
            } else if(found.size() == 1) {
                VectorClock oldClock = (VectorClock) found.get(0).getVersion();
                clock = oldClock.incremented(0, time.getMilliseconds());
            } else {
                fail("Found multiple versions.");
            }
            byte[] value = SecondaryIndexTestUtils.createTestSerializedValue(item,
                                                                             0,
                                                                             new Date(),
                                                                             "company",
                                                                             true);
            engine.put(key, new Versioned<byte[]>(value, clock), null);
        }
    }

    private ByteArray serializeKey(String key) {
        return new ByteArray(key.getBytes());
    }

    private void assertContains(String... keys) {
        HashSet<String> got = Sets.newLinkedHashSet();
        for(ByteArray serKey: new DefaultIterable<ByteArray>(engine.keys())) {
            got.add(new String(serKey.get()));
        }

        assertEquals(ImmutableSet.of(keys), got);
    }

    private void put(String key, int status, Date lastmod, String company, Boolean feedless) {
        byte[] value = SecondaryIndexTestUtils.createTestSerializedValue(key,
                                                                         status,
                                                                         lastmod,
                                                                         company,
                                                                         feedless);
        engine.put(serializeKey(key), new Versioned<byte[]>(value, new VectorClock()), null);
    }

    public void testCleanupWithCondition() {
        put("a", 1, time.getCurrentDate(), "c1", true);
        put("b", 2, time.getCurrentDate(), "c1", true);

        time.addMilliseconds(Time.MS_PER_DAY + Time.MS_PER_SECOND);
        put("c", 3, time.getCurrentDate(), "c1", true);
        put("d", 4, time.getCurrentDate(), "c1", true);
        put("e", 3, time.getCurrentDate(), "c1", false);

        assertContains("a", "b", "c", "d", "e");

        // now run cleanup
        DataCleanupJob<ByteArray, byte[], byte[]> cleanupJob = new DataCleanupJob<ByteArray, byte[], byte[]>(engine,
                                                                                                             new Semaphore(1),
                                                                                                             Time.MS_PER_DAY,
                                                                                                             time,
                                                                                                             new EventThrottler(1),
                                                                                                             "status in {2, 3} and lastmod < ${date} and feedless = true");

        cleanupJob.run();
        assertContains("a", "c", "d", "e");

        time.addMilliseconds(Time.MS_PER_DAY + Time.MS_PER_SECOND);

        cleanupJob.run();
        assertContains("a", "d", "e");
    }

}
