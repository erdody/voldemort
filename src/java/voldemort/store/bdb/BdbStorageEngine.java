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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StorageEngine;
import voldemort.store.StorageInitializationException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.store.bdb.stats.BdbEnvironmentStats;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseStats;
import com.sleepycat.je.Environment;
import com.sleepycat.je.ForwardCursor;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;

/**
 * A store that uses BDB for persistence
 * 
 * <p>
 * This implementation is intended for bdb-je versions 5+, where the log format
 * was changed and now databases with duplicate support consume much more
 * memory, because internally it generates a composite key with key + value.
 * This makes it impossible to hold all IN nodes in memory, which is a
 * requirement for optimum performance.
 * <p>
 * The idea is mainly to create a composite key with key + version, to make keys
 * unique. A custom comparator is provided to BDB, which checks the original key
 * first and in case of equality it uses the version to untie. Tree is still
 * ordered by original key.
 * 
 */
public class BdbStorageEngine implements StorageEngine<ByteArray, byte[], byte[]> {

    private static final Logger logger = Logger.getLogger(BdbStorageEngine.class);
    private static final Hex hexCodec = new Hex();

    private final String name;
    protected Database bdbDatabase;
    protected final Environment environment;
    private final AtomicBoolean isOpen;
    private final LockMode readLockMode;
    private final BdbEnvironmentStats bdbEnvironmentStats;
    private final AtomicBoolean isTruncating = new AtomicBoolean(false);

    protected final VersionedKeyHandler keyHandler = createKeyHandler();

    public BdbStorageEngine(String name,
                            Environment environment,
                            Database database,
                            BdbRuntimeConfig config) {
        this.name = Utils.notNull(name);
        this.bdbDatabase = Utils.notNull(database);
        this.environment = Utils.notNull(environment);
        this.isOpen = new AtomicBoolean(true);
        this.readLockMode = config.getLockMode();
        this.bdbEnvironmentStats = new BdbEnvironmentStats(environment, config.getStatsCacheTtlMs());
    }

    public String getName() {
        return name;
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        try {
            ForwardCursor cursor = getBdbDatabase().openCursor(null, null);
            return new BdbEntriesIterator(cursor);
        } catch(DatabaseException e) {
            logger.error("While retrieving entries", e);
            throw new PersistenceFailureException(e);
        }
    }

    public ClosableIterator<ByteArray> keys() {
        try {
            ForwardCursor cursor = getBdbDatabase().openCursor(null, null);
            return new BdbKeysIterator(cursor);
        } catch(DatabaseException e) {
            logger.error("While retrieving keys", e);
            throw new PersistenceFailureException(e);
        }
    }

    public void truncate() {

        if(isTruncating.compareAndSet(false, true)) {
            Transaction transaction = null;
            boolean succeeded = false;

            try {
                transaction = this.environment.beginTransaction(null, null);

                // close current bdbDatabase first
                bdbDatabase.close();

                // truncate the database
                environment.truncateDatabase(transaction, this.getName(), false);

                succeeded = true;
            } catch(DatabaseException e) {
                logger.error("While truncating", e);
                throw new VoldemortException("Failed to truncate Bdb store " + getName(), e);

            } finally {

                commitOrAbort(succeeded, transaction);

                // reopen the bdb database for future queries.
                if(reopenBdbDatabase()) {
                    isTruncating.compareAndSet(true, false);
                } else {
                    throw new VoldemortException("Failed to reopen Bdb Database after truncation, All request will fail on store "
                                                 + getName());
                }
            }
        } else {
            throw new VoldemortException("Store " + getName()
                                         + " is already truncating, cannot start another one.");
        }
    }

    private void commitOrAbort(boolean succeeded, Transaction transaction) {
        try {
            if(succeeded) {
                attemptCommit(transaction);
            } else {
                attemptAbort(transaction);
            }
        } catch(Exception e) {
            logger.error("While " + (succeeded ? "committing" : "aborting") + " transaction "
                         + transaction, e);
        }
    }

    /**
     * Reopens the bdb Database after a successful truncate operation.
     */
    protected boolean reopenBdbDatabase() {
        try {
            bdbDatabase = environment.openDatabase(null,
                                                   this.getName(),
                                                   this.bdbDatabase.getConfig());
            return true;
        } catch(DatabaseException e) {
            throw new StorageInitializationException("Failed to reinitialize BdbStorageEngine for store:"
                                                             + getName() + " after truncation.",
                                                     e);
        }
    }

    public List<Version> getVersions(ByteArray key) {
        return get(key, null, readLockMode, VERSION_EXTRACTOR);
    }

    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms)
            throws PersistenceFailureException {
        return get(key, transforms, readLockMode, VERSIONED_EXTRACTOR);
    }

    private <X> List<X> get(ByteArray key,
                            @SuppressWarnings("unused") byte[] transforms,
                            LockMode lockMode,
                            EntryValueExtractor<X> extractor) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);

        Cursor cursor = null;
        try {
            cursor = getBdbDatabase().openCursor(null, null);
            return get(cursor, key, lockMode, extractor);
        } catch(DatabaseException e) {
            logger.error("While getting", e);
            throw new PersistenceFailureException(e);
        } finally {
            attemptClose(cursor);
        }
    }

    /**
     * truncate() operation mandates that all opened Database be closed before
     * attempting truncation.
     * <p>
     * This method throws an exception while truncation is happening to any
     * request attempting in parallel with store truncation.
     * 
     * @return
     */
    protected Database getBdbDatabase() {
        if(isTruncating.get()) {
            throw new VoldemortException("Bdb Store " + getName()
                                         + " is currently truncating cannot serve any request.");
        }

        return bdbDatabase;
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<ByteArray, List<Versioned<byte[]>>> result = StoreUtils.newEmptyHashMap(keys);
        Cursor cursor = null;
        try {
            cursor = getBdbDatabase().openCursor(null, null);
            for(ByteArray key: keys) {
                List<Versioned<byte[]>> values = get(cursor, key, readLockMode, VERSIONED_EXTRACTOR);
                if(!values.isEmpty())
                    result.put(key, values);
            }
        } catch(DatabaseException e) {
            logger.error("In getAll", e);
            throw new PersistenceFailureException(e);
        } finally {
            attemptClose(cursor);
        }
        return result;
    }

    private static interface EntryValueExtractor<X> {

        boolean processValues();

        X extract(DatabaseEntry keyEntry, DatabaseEntry valueEntry);

    }

    private static EntryValueExtractor<Versioned<byte[]>> VERSIONED_EXTRACTOR = new EntryValueExtractor<Versioned<byte[]>>() {

        public boolean processValues() {
            return true;
        }

        public Versioned<byte[]> extract(DatabaseEntry keyEntry, DatabaseEntry valueEntry) {
            return VersionedKeyHandler.toVersionedValue(valueEntry.getData(), keyEntry.getData());
        }
    };

    private static EntryValueExtractor<Version> VERSION_EXTRACTOR = new EntryValueExtractor<Version>() {

        public boolean processValues() {
            return false;
        }

        public Version extract(DatabaseEntry keyEntry, DatabaseEntry valueEntry) {
            return VersionedKeyHandler.getVectorClock(keyEntry.getData());
        }
    };

    private <X> List<X> get(Cursor cursor,
                            ByteArray key,
                            LockMode lockMode,
                            EntryValueExtractor<X> extractor) throws DatabaseException {
        StoreUtils.assertValidKey(key);

        DatabaseEntry keyEntry = new DatabaseEntry(keyHandler.getKeyWithEmptyPayload(key.get()));
        DatabaseEntry valueEntry = extractor.processValues() ? new DatabaseEntry() : PARTIAL_ENTRY;
        List<X> results = Lists.newArrayList();
        for(OperationStatus status = cursor.getSearchKeyRange(keyEntry, valueEntry, lockMode); status == OperationStatus.SUCCESS
                                                                                               && keyHandler.matchesOriginalKey(keyEntry.getData(),
                                                                                                                                key.get()); status = cursor.getNext(keyEntry,
                                                                                                                                                                    valueEntry,
                                                                                                                                                                    lockMode)) {
            results.add(extractor.extract(keyEntry, valueEntry));
        }
        return results;
    }

    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);

        DatabaseEntry keyEntry = new DatabaseEntry(keyHandler.getKeyWithEmptyPayload(key.get()));
        boolean succeeded = false;
        Transaction transaction = null;
        Cursor cursor = null;
        try {
            transaction = this.environment.beginTransaction(null, null);

            // Check existing values
            // if there is a version obsoleted by this value delete it
            // if there is a version later than this one, throw an exception
            cursor = getBdbDatabase().openCursor(transaction, null);

            for(OperationStatus status = cursor.getSearchKeyRange(keyEntry,
                                                                  PARTIAL_ENTRY,
                                                                  LockMode.RMW); status == OperationStatus.SUCCESS
                                                                                 && keyHandler.matchesOriginalKey(keyEntry.getData(),
                                                                                                                  key.get()); status = cursor.getNext(keyEntry,
                                                                                                                                                      PARTIAL_ENTRY,
                                                                                                                                                      LockMode.RMW)) {

                VectorClock clock = new VectorClock(keyEntry.getData());
                Occurred occurred = value.getVersion().compare(clock);
                if(occurred == Occurred.BEFORE) {
                    throw new ObsoleteVersionException("Key "
                                                       + new String(hexCodec.encode(key.get()))
                                                       + " "
                                                       + value.getVersion().toString()
                                                       + " is obsolete, it is no greater than the current version of "
                                                       + clock + ".");
                } else if(occurred == Occurred.AFTER) {
                    // best effort delete of obsolete previous value!
                    cursor.delete();
                }
            }

            // Okay so we cleaned up all the prior stuff, so now we are good to
            // insert the new thing
            DatabaseEntry valueEntry = new DatabaseEntry(value.getValue());
            keyEntry = new DatabaseEntry(serializeKey(key, value));

            OperationStatus status = cursor.put(keyEntry, valueEntry);
            if(status != OperationStatus.SUCCESS)
                throw new PersistenceFailureException("Put operation failed with status: " + status);

            succeeded = true;
        } catch(DatabaseException e) {
            logger.error("While putting", e);
            throw new PersistenceFailureException(e);
        } finally {
            attemptClose(cursor);
            if(succeeded)
                attemptCommit(transaction);
            else
                attemptAbort(transaction);
        }
    }

    private final static DatabaseEntry PARTIAL_ENTRY;
    static {
        PARTIAL_ENTRY = new DatabaseEntry(ArrayUtils.EMPTY_BYTE_ARRAY);
        PARTIAL_ENTRY.setPartial(0, 0, true);
    }

    public boolean delete(ByteArray key, Version version) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        Cursor cursor = null;
        Transaction transaction = null;
        try {
            transaction = this.environment.beginTransaction(null, null);
            DatabaseEntry keyEntry = new DatabaseEntry(keyHandler.getKeyWithEmptyPayload(key.get()));
            cursor = getBdbDatabase().openCursor(transaction, null);

            boolean deleted = false;

            for(OperationStatus status = cursor.getSearchKeyRange(keyEntry,
                                                                  PARTIAL_ENTRY,
                                                                  LockMode.READ_UNCOMMITTED); status == OperationStatus.SUCCESS
                                                                                              && keyHandler.matchesOriginalKey(keyEntry.getData(),
                                                                                                                               key.get()); status = cursor.getNext(keyEntry,
                                                                                                                                                                   PARTIAL_ENTRY,
                                                                                                                                                                   LockMode.READ_UNCOMMITTED)) {

                // if version is null no comparison is necessary
                if(new VectorClock(keyEntry.getData()).compare(version) == Occurred.BEFORE) {
                    cursor.delete();
                    deleted = true;
                }
            }

            return deleted;
        } catch(DatabaseException e) {
            logger.error("While deleting", e);
            throw new PersistenceFailureException(e);
        } finally {
            try {
                attemptClose(cursor);
            } finally {
                attemptCommit(transaction);
            }
        }
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if(o == null || !Store.class.isAssignableFrom(o.getClass()))
            return false;
        Store<?, ?, ?> s = (Store<?, ?, ?>) o;
        return s.getName().equals(s.getName());
    }

    public void close() throws PersistenceFailureException {
        try {
            if(this.isOpen.compareAndSet(true, false)) {
                this.getBdbDatabase().close();
            }
        } catch(DatabaseException e) {
            logger.error("While closing", e);
            throw new PersistenceFailureException("Shutdown failed.", e);
        }
    }

    private void attemptAbort(Transaction transaction) {
        try {
            if(transaction != null)
                transaction.abort();
        } catch(Exception e) {
            logger.error("Abort failed!", e);
        }
    }

    private void attemptCommit(Transaction transaction) {
        try {
            transaction.commit();
        } catch(DatabaseException e) {
            logger.error("Transaction commit failed!", e);
            attemptAbort(transaction);
            throw new PersistenceFailureException(e);
        }
    }

    private static void attemptClose(Cursor cursor) {
        try {
            if(cursor != null)
                cursor.close();
        } catch(DatabaseException e) {
            logger.error("Error closing cursor.", e);
            throw new PersistenceFailureException(e.getMessage(), e);
        }
    }

    public DatabaseStats getStats(boolean setFast) {
        try {
            StatsConfig config = new StatsConfig();
            config.setFast(setFast);
            return this.getBdbDatabase().getStats(config);
        } catch(DatabaseException e) {
            logger.error("While retrieving stats", e);
            throw new VoldemortException(e);
        }
    }

    @JmxOperation(description = "A variety of quickly computable stats about the BDB for this store.")
    public String getBdbStats() {
        return getBdbStats(true);
    }

    @JmxOperation(description = "A variety of stats about the BDB for this store.")
    public String getBdbStats(boolean fast) {
        String dbStats = getStats(fast).toString();
        logger.debug(dbStats);
        return dbStats;
    }

    public BdbEnvironmentStats getBdbEnvironmentStats() {
        return bdbEnvironmentStats;
    }

    protected static abstract class BdbIterator<T> implements ClosableIterator<T> {

        final ForwardCursor cursor;

        private T current;

        private volatile boolean isClosed;
        private volatile boolean isInited;
        private DatabaseEntry valueEntry;
        private DatabaseEntry keyEntry;

        public BdbIterator(ForwardCursor cursor, boolean noValues) {
            this.cursor = cursor;
            this.isClosed = false;
            this.isInited = false;

            keyEntry = new DatabaseEntry();
            valueEntry = new DatabaseEntry();
            if(noValues)
                valueEntry.setPartial(true);
        }

        protected abstract T get(DatabaseEntry key, DatabaseEntry value);

        protected OperationStatus moveCursor(DatabaseEntry key, DatabaseEntry value)
                throws DatabaseException {
            return cursor.getNext(key, value, LockMode.READ_UNCOMMITTED);
        }

        public final boolean hasNext() {
            if(!isInited)
                initCursor();
            return current != null;
        }

        protected void initCursor() {
            if(!isInited) {
                try {
                    OperationStatus opStatus = moveCursor(keyEntry, valueEntry);
                    if(opStatus == OperationStatus.SUCCESS) {
                        current = get(keyEntry, valueEntry);
                    }
                    isInited = true;
                } catch(DatabaseException e) {
                    logger.error("While initializing cursor", e);
                    throw new PersistenceFailureException(e);
                }
            }
        }

        public final T next() {
            if(!isInited)
                initCursor();
            if(isClosed)
                throw new PersistenceFailureException("Call to next() on a closed iterator.");

            T previous = current;
            try {
                OperationStatus opStatus = moveCursor(keyEntry, valueEntry);
                if(opStatus == OperationStatus.SUCCESS) {
                    current = get(keyEntry, valueEntry);
                } else {
                    current = null;
                }

                return previous;
            } catch(DatabaseException e) {
                logger.error("While iterating cursor", e);
                throw new PersistenceFailureException(e);
            }
        }

        public final void remove() {
            throw new UnsupportedOperationException("No removal y'all.");
        }

        public final void close() {
            try {
                cursor.close();
                isClosed = true;
            } catch(DatabaseException e) {
                logger.error("While closing cursor", e);
            }
        }

        @Override
        protected final void finalize() {
            if(!isClosed) {
                logger.error("Failure to close cursor, will be forcably closed.");
                close();
            }
        }
    }

    private class BdbKeysIterator extends BdbIterator<ByteArray> {

        public BdbKeysIterator(ForwardCursor cursor) {
            super(cursor, true);
        }

        @Override
        protected ByteArray get(DatabaseEntry key, DatabaseEntry value) {
            return new ByteArray(keyHandler.getOriginalKey(key.getData()));
        }

    }

    private class BdbEntriesIterator extends BdbIterator<Pair<ByteArray, Versioned<byte[]>>> {

        public BdbEntriesIterator(ForwardCursor cursor) {
            super(cursor, false);
        }

        @Override
        protected Pair<ByteArray, Versioned<byte[]>> get(DatabaseEntry key, DatabaseEntry value) {
            byte[] keyBytes = keyHandler.getOriginalKey(key.getData());
            return Pair.create(new ByteArray(keyBytes),
                               VersionedKeyHandler.toVersionedValue(value.getData(), key.getData()));
        }

    }

    public boolean isPartitionAware() {
        return false;
    }

    public ClosableIterator<KeyMatch<ByteArray>> keys(String query) {
        throw new UnsupportedOperationException("No secondary index support.");
    }

    protected VersionedKeyHandler createKeyHandler() {
        return new VersionedKeyHandler();
    }

    /**
     * Class in charge of understanding a composite key.
     * <p>
     * It prepends a VectorClock object (with a size byte) to the original key.
     * The idea is that this class can be extended to add more information
     * (besides the version) to the "payload" initial segment (e.g. secondary
     * values)
     */
    public static class VersionedKeyHandler implements Comparator<byte[]> {

        /**
         * @return size for the initial payload segment in the composite key.
         *         After this much bytes, we will have the original key.
         */
        protected int getPayloadSize(byte[] cKey) {
            return VectorClock.getSize(cKey, 0);
        }

        /**
         * Compares original key first and uses payload (binary format) to break
         * ties.
         */
        public int compare(byte[] cKey1, byte[] cKey2) {
            int cSize1 = getPayloadSize(cKey1);
            int cSize2 = getPayloadSize(cKey2);
            int result = ByteArray.compare(cKey1,
                                           cSize1,
                                           cKey1.length - cSize1,
                                           cKey2,
                                           cSize2,
                                           cKey2.length - cSize2);
            return result != 0 ? result : ByteArray.compare(cKey1, 0, cSize1, cKey2, 0, cSize2);
        }

        /**
         * Compares original key only, regardless of the payload.
         */
        public int compareOriginalKeyOnly(byte[] cKey1, byte[] cKey2) {
            int cSize1 = getPayloadSize(cKey1);
            int cSize2 = getPayloadSize(cKey2);
            return ByteArray.compare(cKey1,
                                     cSize1,
                                     cKey1.length - cSize1,
                                     cKey2,
                                     cSize2,
                                     cKey2.length - cSize2);
        }

        /**
         * @return true if the original key in the given composite key matches
         *         the given one.
         */
        public boolean matchesOriginalKey(byte[] cKey, byte[] key) {
            int vSize = getPayloadSize(cKey);
            return (cKey.length - vSize == key.length)
                   && ByteArray.compare(cKey, vSize, key.length, key, 0, key.length) == 0;
        }

        /** @return original key stored in the given composite key */
        public byte[] getOriginalKey(byte[] cKey) {
            return ByteUtils.copy(cKey, getPayloadSize(cKey), cKey.length);
        }

        /**
         * @return a payload with no content. It should be the smallest
         *         possible.
         */
        protected byte[] getEmptyPayload() {
            // this is the smallest VectorClock we can have
            return new byte[11];
        }

        /**
         * Creates a composite key for the given original key with a payload
         * that will be the "smallest". Typically used to use as the beginning
         * in a range query.
         */
        public byte[] getKeyWithEmptyPayload(byte[] key) {
            return ByteUtils.cat(getEmptyPayload(), key);
        }

        /** Extracts the {@link VectorClock} from a composite key */
        protected static VectorClock getVectorClock(byte[] cKey) {
            if(cKey[0] >= 0)
                return new VectorClock(cKey);
            return null;
        }

        /**
         * Creates a {@link Versioned} value, from a given composite key
         */
        public static Versioned<byte[]> toVersionedValue(byte[] value, byte[] cKey) {
            return new Versioned<byte[]>(value, getVectorClock(cKey));
        }

        private static byte[] toBytes(Version version) {
            byte[] versionBytes = null;
            if(version == null)
                versionBytes = new byte[] { -1 };
            else
                versionBytes = ((VectorClock) version).toBytes();
            return versionBytes;
        }

    }

    /**
     * Generate bytes that will be prepended to the original key to create the
     * composite key that will be actually stored.
     */
    protected byte[] assembleKeyPayload(@SuppressWarnings("unused") ByteArray key,
                                        Versioned<byte[]> value) {
        return VersionedKeyHandler.toBytes(value.getVersion());
    }

    private byte[] serializeKey(ByteArray key, Versioned<byte[]> value) {
        return ByteUtils.cat(assembleKeyPayload(key, value), key.get());
    }

}
