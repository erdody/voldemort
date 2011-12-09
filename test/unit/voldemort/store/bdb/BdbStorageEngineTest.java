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

package voldemort.store.bdb;

import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileDeleteStrategy;

import voldemort.TestUtils;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;

public class BdbStorageEngineTest extends AbstractStorageEngineTest {

    private static final LockMode LOCK_MODE = LockMode.DEFAULT;

    private Environment environment;
    private EnvironmentConfig envConfig;
    protected Database database;
    private File tempDir;
    private BdbStorageEngine store;
    private DatabaseConfig databaseConfig;
    private BdbRuntimeConfig runtimeConfig;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.envConfig = new EnvironmentConfig();
        this.envConfig.setTxnNoSync(true);
        this.envConfig.setAllowCreate(true);
        this.envConfig.setTransactional(true);
        this.tempDir = TestUtils.createTempDir();
        this.environment = new Environment(this.tempDir, envConfig);
        this.databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setTransactional(true);
        databaseConfig.setSortedDuplicates(true);
        this.database = environment.openDatabase(null, "test", databaseConfig);
        this.runtimeConfig = new BdbRuntimeConfig();
        runtimeConfig.setLockMode(LOCK_MODE);
        this.store = createBdbStorageEngine(runtimeConfig);
    }

    protected BdbStorageEngine createBdbStorageEngine(BdbRuntimeConfig runtimeConfig) {
        return new BdbStorageEngine("test", this.environment, this.database, runtimeConfig);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        try {
            store.close();
            environment.close();
        } finally {
            FileDeleteStrategy.FORCE.delete(tempDir);
        }
    }

    @Override
    public StorageEngine<ByteArray, byte[], byte[]> getStorageEngine() {
        return store;
    }

    public void testPersistence() throws Exception {
        ByteArray key = new ByteArray("abc".getBytes());
        Versioned<byte[]> value = new Versioned<byte[]>(getValue());

        this.store.put(key, value, null);
        this.store.close();
        this.environment.close();
        this.environment = new Environment(this.tempDir, envConfig);
        this.database = environment.openDatabase(null, "test", databaseConfig);
        this.store = createBdbStorageEngine(runtimeConfig);
        List<Versioned<byte[]>> vals = store.get(key, null);
        assertEquals(1, vals.size());
        TestUtils.bytesEqual(value.getValue(), vals.get(0).getValue());
    }

    public void testEquals() {
        String name = "someName";
        assertEquals(new BdbStorageEngine(name, environment, database, runtimeConfig),
                     new BdbStorageEngine(name, environment, database, runtimeConfig));
    }

    public void testNullConstructorParameters() {
        try {
            new BdbStorageEngine(null, environment, database, runtimeConfig);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("No exception thrown for null name.");
        try {
            new BdbStorageEngine("name", null, database, runtimeConfig);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("No exception thrown for null environment.");
        try {
            new BdbStorageEngine("name", environment, null, runtimeConfig);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("No exception thrown for null database.");
    }

    public void testConcurrentReadAndPut() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        final CountDownLatch latch = new CountDownLatch(10);
        final AtomicBoolean returnedEmpty = new AtomicBoolean(false);
        final byte[] keyBytes = "foo".getBytes();
        final byte[] valueBytes = getValue();
        store.put(new ByteArray(keyBytes), new Versioned<byte[]>(valueBytes), null);

        for(int i = 0; i < 10; i++) {
            executor.submit(new Runnable() {

                public void run() {
                    try {
                        for(int j = 0; j < 1000 && !returnedEmpty.get(); j++) {
                            List<Versioned<byte[]>> vals = store.get(new ByteArray(keyBytes), null);
                            if(vals.size() == 0 && j > 1)
                                returnedEmpty.set(true);
                            else {
                                VectorClock v = (VectorClock) vals.get(0).getVersion();
                                v.incrementVersion(0, System.currentTimeMillis());
                                try {
                                    store.put(new ByteArray(keyBytes), new Versioned<byte[]>(valueBytes, v), null);
                                } catch(ObsoleteVersionException e) {
                                    // Ignore these
                                }
                            }
                        }
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        latch.await();
        assertFalse("Should not have seen any empty results", returnedEmpty.get());
    }

    public void testSimultaneousIterationAndModification() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        final Random rand = new Random();
        final AtomicInteger count = new AtomicInteger(0);
        final AtomicBoolean keepRunning = new AtomicBoolean(true);
        final int numElems = 300;
        final List<ByteArray> keys = getKeys(numElems);
        final List<byte[]> values = getValues(numElems);

        // start a thread to put entries
        Future<?> putFuture = executor.submit(new Callable<Void>() {

            public Void call() throws Exception {
                for(int i = 0; i < numElems; i++) {
                    ByteArray key = keys.get(i);
                    Versioned<byte[]> value = Versioned.value(values.get(i));
                    store.put(key, value, null);
                    count.incrementAndGet();
                }
                return null;
            }
        });

        // start a thread to remove entries randomly
        Future<Void> deleteFuture = executor.submit(new Callable<Void>() {

            public Void call() throws Exception {
                while(keepRunning.get()) {
                    if(count.get() > 0) {
                        int idx = rand.nextInt(count.get());
                        ByteArray key = keys.get(idx);
                        store.delete(key, new VectorClock());
                    }
                }
                return null;
            }
        });

        // start a thread to iterate over all the entries
        Future<Void> iterFuture = executor.submit(new Callable<Void>() {

            public Void call() throws Exception {
                while(keepRunning.get()) {
                    ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iter = store.entries();
                    while(iter.hasNext())
                        iter.next();
                    iter.close();
                }
                return null;
            }
        });

        putFuture.get();
        keepRunning.set(false);

        // check no exceptions thrown
        deleteFuture.get();
        iterFuture.get();

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Override
    public void testTruncate() throws Exception {
        super.testTruncate();

        if(isSecondaryIndexEnabled()) {
            // just check secondary index was cleared
            secIdxTestUtils.assertQueryReturns(secIdxTestUtils.query("status",
                                                                     (byte) 0,
                                                                     Byte.MAX_VALUE));
        }
    }

}
