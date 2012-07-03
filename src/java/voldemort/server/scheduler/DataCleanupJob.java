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

package voldemort.server.scheduler;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import voldemort.store.StorageEngine;
import voldemort.store.StorageEngine.KeyMatch;
import voldemort.utils.ClosableIterator;
import voldemort.utils.EventThrottler;
import voldemort.utils.Pair;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Expire old data
 * 
 * 
 */
public class DataCleanupJob<K, V, T> implements Runnable {

    private static final Logger logger = Logger.getLogger(DataCleanupJob.class);

    private final StorageEngine<K, V, T> store;
    private final Semaphore cleanupPermits;
    private final long maxAgeMs;
    private final Time time;
    private final EventThrottler throttler;
    private final String condition;

    public DataCleanupJob(StorageEngine<K, V, T> store,
                          Semaphore cleanupPermits,
                          long maxAgeMs,
                          Time time,
                          EventThrottler throttler,
                          String condition) {
        this.store = Utils.notNull(store);
        this.cleanupPermits = Utils.notNull(cleanupPermits);
        this.maxAgeMs = maxAgeMs;
        this.time = time;
        this.throttler = throttler;
        this.condition = condition;
    }

    public void run() {
        acquireCleanupPermit();

        CleanupMethod<?> cm = null;
        try {
            logger.info("Starting data cleanup on store \"" + store.getName() + "\"...");
            int deleted = 0;

            if(condition != null)
                cm = new SecIdxMethod();
            else
                cm = new VectorClockMethod();

            cm.init();
            while(cm.iterator.hasNext()) {
                // check if we have been interrupted
                if(Thread.currentThread().isInterrupted()) {
                    logger.info("Datacleanup job halted.");
                    return;
                }

                if(cm.processNext()) {
                    deleted++;
                    if(deleted % 10000 == 0)
                        logger.debug("Deleted item " + deleted);
                }

                // throttle on number of entries.
                throttler.maybeThrottle(1);
            }
            logger.info("Data cleanup on store \"" + store.getName() + "\" is complete; " + deleted
                        + " items deleted.");
        } catch(Exception e) {
            logger.error("Error in data cleanup job for store " + store.getName() + ": ", e);
        } finally {
            closeIterator(cm);
            logger.info("Releasing lock  after data cleanup on \"" + store.getName() + "\".");
            this.cleanupPermits.release();
        }
    }

    abstract class CleanupMethod<IT> {

        ClosableIterator<IT> iterator;
        final long nowMs;

        public CleanupMethod() {
            this.nowMs = time.getMilliseconds();
        }

        public abstract boolean processNext();

        public abstract void init();

    }

    class VectorClockMethod extends CleanupMethod<Pair<K, Versioned<V>>> {

        @Override
        public boolean processNext() {
            Pair<K, Versioned<V>> keyAndVal = iterator.next();
            VectorClock clock = (VectorClock) keyAndVal.getSecond().getVersion();
            if(nowMs - clock.getTimestamp() > maxAgeMs) {
                store.delete(keyAndVal.getFirst(), clock);
                return true;
            }
            return false;
        }

        @Override
        public void init() {
            iterator = store.entries();
        }

    }

    class SecIdxMethod extends CleanupMethod<KeyMatch<K>> {

        private final static String LIMIT_PLACEHOLDER = "${date}";

        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

        @Override
        public boolean processNext() {
            KeyMatch<K> keyMatch = iterator.next();
            if(keyMatch != null) {
                for(Version version: keyMatch.getMatchingVersions()) {
                    store.delete(keyMatch.getKey(), version);
                }
            }
            return keyMatch != null;
        }

        @Override
        public void init() {
            String formattedDate = dateFormat.format(new Date(nowMs - maxAgeMs));
            String formattedCondition = condition.replace(LIMIT_PLACEHOLDER, "'" + formattedDate
                                                                             + "'");
            iterator = store.keys(formattedCondition);
        }

    }

    private void closeIterator(CleanupMethod<?> cm) {
        try {
            if(cm != null)
                cm.iterator.close();
        } catch(Exception e) {
            logger.error("Error in closing iterator " + store.getName() + " ", e);
        }
    }

    private void acquireCleanupPermit() {
        logger.info("Acquiring lock to perform data cleanup on \"" + store.getName() + "\".");
        try {
            this.cleanupPermits.acquire();
        } catch(InterruptedException e) {
            throw new IllegalStateException("Datacleanup interrupted while waiting for cleanup permit.",
                                            e);
        }
    }

}
