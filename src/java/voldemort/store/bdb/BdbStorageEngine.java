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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.secondary.RangeQuery;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.Serializer;
import voldemort.serialization.VersionedSerializer;
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
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;

/**
 * A store that uses BDB for persistence
 * 
 * 
 */
public class BdbStorageEngine implements StorageEngine<ByteArray, byte[], byte[]> {

    private static final Logger logger = Logger.getLogger(BdbStorageEngine.class);
    private static final Hex hexCodec = new Hex();

    private final String name;
    protected Database bdbDatabase;
    protected final Environment environment;
    private final VersionedSerializer<byte[]> versionedSerializer;
    private final AtomicBoolean isOpen;
    private final LockMode readLockMode;
    private final Serializer<Version> versionSerializer;
    private final BdbEnvironmentStats bdbEnvironmentStats;
    private final AtomicBoolean isTruncating = new AtomicBoolean(false);

    public BdbStorageEngine(String name,
                            Environment environment,
                            Database database,
                            BdbRuntimeConfig config) {
        this.name = Utils.notNull(name);
        this.bdbDatabase = Utils.notNull(database);
        this.environment = Utils.notNull(environment);
        this.versionedSerializer = new VersionedSerializer<byte[]>(new IdentitySerializer());
        this.versionSerializer = new Serializer<Version>() {

            public byte[] toBytes(Version object) {
                return ((VectorClock) object).toBytes();
            }

            public Version toObject(byte[] bytes) {
                return versionedSerializer.getVersion(bytes);
            }
        };
        this.isOpen = new AtomicBoolean(true);
        this.readLockMode = config.getLockMode();
        this.bdbEnvironmentStats = new BdbEnvironmentStats(environment, config.getStatsCacheTtlMs());
    }

    public String getName() {
        return name;
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        try {
            Cursor cursor = getBdbDatabase().openCursor(null, null);
            return new BdbEntriesIterator(cursor);
        } catch(DatabaseException e) {
            logger.error("While retrieving entries", e);
            throw new PersistenceFailureException(e);
        }
    }

    public ClosableIterator<ByteArray> keys() {
        try {
            Cursor cursor = getBdbDatabase().openCursor(null, null);
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
                closeInternal(bdbDatabase);

                // truncate the database
                environment.truncateDatabase(transaction, this.getName(), false);

                succeeded = truncatePostActions(transaction);
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

    protected boolean truncatePostActions(Transaction tx) {
        return true;
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
        return get(key, null, readLockMode, versionSerializer);
    }

    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms)
            throws PersistenceFailureException {
        return get(key, transforms, readLockMode, versionedSerializer);
    }

    private <T> List<T> get(ByteArray key,
                            @SuppressWarnings("unused") byte[] transforms,
                            LockMode lockMode,
                            Serializer<T> serializer) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);

        Cursor cursor = null;
        try {
            cursor = getBdbDatabase().openCursor(null, null);
            return get(cursor, key, lockMode, serializer);
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
    private Database getBdbDatabase() {
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
                List<Versioned<byte[]>> values = get(cursor, key, readLockMode, versionedSerializer);
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

    private static <T> List<T> get(Cursor cursor,
                                   ByteArray key,
                                   LockMode lockMode,
                                   Serializer<T> serializer) throws DatabaseException {
        StoreUtils.assertValidKey(key);

        DatabaseEntry keyEntry = new DatabaseEntry(key.get());
        DatabaseEntry valueEntry = new DatabaseEntry();
        List<T> results = Lists.newArrayList();

        for(OperationStatus status = cursor.getSearchKey(keyEntry, valueEntry, lockMode); status == OperationStatus.SUCCESS; status = cursor.getNextDup(keyEntry,
                                                                                                                                                        valueEntry,
                                                                                                                                                        lockMode)) {
            results.add(serializer.toObject(valueEntry.getData()));
        }
        return results;
    }

    protected Versioned<byte[]> getVersionedValue(byte[] rawValue) {
        return versionedSerializer.toObject(rawValue);
    }

    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);

        DatabaseEntry keyEntry = new DatabaseEntry(key.get());
        boolean succeeded = false;
        Transaction transaction = null;
        Cursor cursor = null;
        try {
            transaction = this.environment.beginTransaction(null, null);

            // Check existing values
            // if there is a version obsoleted by this value delete it
            // if there is a version later than this one, throw an exception
            DatabaseEntry valueEntry = new DatabaseEntry();
            cursor = getBdbDatabase().openCursor(transaction, null);

            List<byte[]> deletedVals = Lists.newArrayList();
            List<byte[]> remainingVals = Lists.newArrayList();

            for(OperationStatus status = cursor.getSearchKey(keyEntry, valueEntry, LockMode.RMW); status == OperationStatus.SUCCESS; status = cursor.getNextDup(keyEntry,
                                                                                                                                                                valueEntry,
                                                                                                                                                                LockMode.RMW)) {
                VectorClock clock = new VectorClock(valueEntry.getData());
                Occurred occured = value.getVersion().compare(clock);
                if(occured == Occurred.BEFORE) {
                    throw new ObsoleteVersionException("Key "
                                                       + new String(hexCodec.encode(key.get()))
                                                       + " "
                                                       + value.getVersion().toString()
                                                       + " is obsolete, it is no greater than the current version of "
                                                       + clock + ".");
                } else if(occured == Occurred.AFTER) {
                    // best effort delete of obsolete previous value!
                    deletedVals.add(valueEntry.getData());
                    cursor.delete();
                } else {
                    remainingVals.add(valueEntry.getData());
                }
            }

            // Okay so we cleaned up all the prior stuff, so now we are good to
            // insert the new thing
            valueEntry = new DatabaseEntry(versionedSerializer.toBytes(value));
            OperationStatus status = cursor.put(keyEntry, valueEntry);
            if(status != OperationStatus.SUCCESS)
                throw new PersistenceFailureException("Put operation failed with status: " + status);

            putPostActions(transaction, key, deletedVals, remainingVals, value, transforms);

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

    protected void putPostActions(Transaction transaction,
                                  ByteArray key,
                                  List<byte[]> deletedVals,
                                  List<byte[]> remainingVals,
                                  Versioned<byte[]> value,
                                  byte[] transforms) {}

    public boolean delete(ByteArray key, Version version) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        Cursor cursor = null;
        Transaction transaction = null;
        try {
            transaction = this.environment.beginTransaction(null, null);
            DatabaseEntry keyEntry = new DatabaseEntry(key.get());
            DatabaseEntry valueEntry = new DatabaseEntry();
            cursor = getBdbDatabase().openCursor(transaction, null);
            OperationStatus status = cursor.getSearchKey(keyEntry,
                                                         valueEntry,
                                                         LockMode.READ_UNCOMMITTED);

            List<byte[]> deletedVals = Lists.newArrayList();
            List<byte[]> remainingVals = Lists.newArrayList();
            while(status == OperationStatus.SUCCESS) {
                // if version is null no comparison is necessary
                if(new VectorClock(valueEntry.getData()).compare(version) == Occurred.BEFORE) {
                    cursor.delete();
                    deletedVals.add(valueEntry.getData());
                } else {
                    remainingVals.add(valueEntry.getData());
                }
                status = cursor.getNextDup(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED);
            }
            deletePostActions(transaction, key, deletedVals, remainingVals);
            return !deletedVals.isEmpty();
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

    protected void deletePostActions(Transaction transaction,
                                     ByteArray key,
                                     List<byte[]> deletedVals,
                                     List<byte[]> remainingVals) {}

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
                closeInternal(this.getBdbDatabase());
            }
        } catch(DatabaseException e) {
            logger.error("While closing", e);
            throw new PersistenceFailureException("Shutdown failed.", e);
        }
    }

    protected void closeInternal(Database database) {
        database.close();
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

        final Cursor cursor;

        private T current;
        private volatile boolean isClosed;
        private volatile boolean isInited;
        private DatabaseEntry valueEntry;
        private DatabaseEntry keyEntry;

        public BdbIterator(Cursor cursor, boolean noValues) {
            this.cursor = cursor;
            this.isClosed = false;
            this.isInited = false;

            keyEntry = new DatabaseEntry();
            valueEntry = new DatabaseEntry();
            if(noValues)
                valueEntry.setPartial(true);
        }

        protected abstract T get(DatabaseEntry key, DatabaseEntry value);

        protected abstract OperationStatus moveCursor(DatabaseEntry key,
                                                      DatabaseEntry value,
                                                      boolean isFirst) throws DatabaseException;

        public final boolean hasNext() {
            if(!isInited)
                initCursor();
            return current != null;
        }

        protected void initCursor() {
            if(!isInited) {
                try {
                    OperationStatus opStatus = moveCursor(keyEntry, valueEntry, true);
                    if(opStatus == OperationStatus.SUCCESS)
                        current = get(keyEntry, valueEntry);
                    isInited = true;
                } catch(DatabaseException e) {
                    logger.error("While initializaing cursor", e);
                    throw new PersistenceFailureException(e);
                }
            }
        }

        public final T next() {
            if(!isInited)
                initCursor();
            if(isClosed)
                throw new PersistenceFailureException("Call to next() on a closed iterator.");

            try {
                OperationStatus opStatus = moveCursor(keyEntry, valueEntry, false);
                T previous = current;
                if(opStatus != OperationStatus.SUCCESS)
                    current = null;
                else
                    current = get(keyEntry, valueEntry);

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

    private static class BdbKeysIterator extends BdbIterator<ByteArray> {

        public BdbKeysIterator(Cursor cursor) {
            super(cursor, true);
        }

        @Override
        protected ByteArray get(DatabaseEntry key, DatabaseEntry value) {
            return new ByteArray(key.getData());
        }

        @Override
        protected OperationStatus moveCursor(DatabaseEntry key, DatabaseEntry value, boolean isFirst)
                throws DatabaseException {
            return cursor.getNextNoDup(key, value, LockMode.READ_UNCOMMITTED);
        }

    }

    private static class BdbEntriesIterator extends BdbIterator<Pair<ByteArray, Versioned<byte[]>>> {

        public BdbEntriesIterator(Cursor cursor) {
            super(cursor, false);
        }

        @Override
        protected Pair<ByteArray, Versioned<byte[]>> get(DatabaseEntry key, DatabaseEntry value) {
            VectorClock clock = new VectorClock(value.getData());
            byte[] bytes = ByteUtils.copy(value.getData(),
                                          clock.sizeInBytes(),
                                          value.getData().length);
            return Pair.create(new ByteArray(key.getData()), new Versioned<byte[]>(bytes, clock));
        }

        @Override
        protected OperationStatus moveCursor(DatabaseEntry key, DatabaseEntry value, boolean isFirst)
                throws DatabaseException {
            return cursor.getNext(key, value, LockMode.READ_UNCOMMITTED);
        }
    }

    public boolean isPartitionAware() {
        return false;
    }

    public Set<ByteArray> getAllKeys(RangeQuery query) {
        throw new UnsupportedOperationException("No secondary index support.");
    }
}
