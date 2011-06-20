package voldemort.store.bdb;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import voldemort.annotations.Experimental;
import voldemort.secondary.RangeQuery;
import voldemort.secondary.SecondaryIndexProcessor;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StorageInitializationException;
import voldemort.store.StoreCapabilityType;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * {@link BdbStorageEngine} extension that provides secondary index support.
 * <p>
 * Since Voldemort requires to be able to hold many values per key (concurrent
 * versions), the native BDB secondary index feature cannot be used. Instead, we
 * crate one primary BDB database per secondary index, and keep it in synch
 * manually.
 */
@Experimental
public class BdbStorageEngineSI extends BdbStorageEngine {

    private static final Logger logger = Logger.getLogger(BdbStorageEngine.class);

    private Map<String, Database> secDbsByField = Maps.newLinkedHashMap();
    private Map<String, String> secDbNamesByField = Maps.newLinkedHashMap();
    private final SecondaryIndexProcessor secIdxProcessor;

    public BdbStorageEngineSI(String name,
                              Environment environment,
                              Database database,
                              boolean cursorPreload,
                              SecondaryIndexProcessor secIdxProcessor) {
        super(name, environment, database, cursorPreload);
        this.secIdxProcessor = secIdxProcessor;

        DatabaseConfig secConfig = new DatabaseConfig();
        secConfig.setAllowCreate(true); // create secondary DB if not present
        secConfig.setTransactional(true);
        // we don't need to have records with the same key, we append the pKey
        // to make it unique and avoid contention.
        secConfig.setSortedDuplicates(false);
        secConfig.setBtreeComparator(CompositeKeyHandler.class);

        for(String fieldName: secIdxProcessor.getSecondaryFields()) {
            String secDbName = getSecDbName(database, fieldName);
            logger.info("Opening secondary index '" + fieldName + "' db name: " + secDbName);
            Database secDb = environment.openDatabase(null, secDbName, secConfig);
            secDbsByField.put(fieldName, secDb);
            secDbNamesByField.put(fieldName, secDbName);
        }
    }

    private String getSecDbName(Database primaryDB, String secFieldName) {
        return primaryDB.getDatabaseName() + "__" + secFieldName;
    }

    @Override
    protected boolean reopenBdbDatabase() {
        super.reopenBdbDatabase();
        try {
            for(Entry<String, Database> entry: secDbsByField.entrySet()) {
                Database secDb = entry.getValue();
                entry.setValue(secDb.getEnvironment().openDatabase(null,
                                                                   getSecDbName(bdbDatabase,
                                                                                entry.getKey()),
                                                                   secDb.getConfig()));
            }
            return true;
        } catch(DatabaseException e) {
            throw new StorageInitializationException("Failed to reinitialize BdbStorageEngine for store:"
                                                             + getName() + " after truncation.",
                                                     e);
        }
    }

    @Override
    protected void closeInternal(Database database) {
        for(Entry<String, Database> entry: secDbsByField.entrySet()) {
            entry.getValue().close();
        }
        super.closeInternal(database);
    }

    @Override
    public Set<ByteArray> getAllKeys(RangeQuery query) {
        Database secDb = secDbsByField.get(query.getField());
        Cursor cursor = secDb.openCursor(null, null);
        Set<ByteArray> result = Sets.newHashSet();

        byte[] start = (byte[]) query.getStart();
        byte[] end = (byte[]) query.getEnd();

        BdbKeysRangeIterator it = new BdbKeysRangeIterator(cursor, start, end);
        try {
            while(it.hasNext()) {
                result.add(it.next());
            }
        } finally {
            it.close();
        }
        return result;
    }

    private static class BdbKeysRangeIterator extends BdbIterator<ByteArray> {

        private final byte[] from;
        private final byte[] to;

        public BdbKeysRangeIterator(Cursor cursor, byte[] from, byte[] to) {
            super(cursor, false);
            this.from = CompositeKeyHandler.createCompositeKey(from, new byte[0]);
            this.to = CompositeKeyHandler.createCompositeKey(to, new byte[0]);
        }

        @Override
        protected ByteArray get(DatabaseEntry key, DatabaseEntry value) {
            return new ByteArray(CompositeKeyHandler.getPrimaryKey(key.getData()));
        }

        @Override
        protected OperationStatus moveCursor(DatabaseEntry key, DatabaseEntry value, boolean isFirst)
                throws DatabaseException {
            OperationStatus opStatus;
            if(isFirst) {
                key.setData(from);
                opStatus = cursor.getSearchKeyRange(key, value, null);
            } else {
                opStatus = cursor.getNext(key, value, null);
            }

            if(opStatus == OperationStatus.SUCCESS
               && CompositeKeyHandler.compareSecondaryKeys(key.getData(), to) > 0) {
                return OperationStatus.NOTFOUND;
            }
            return opStatus;
        }
    }

    @Override
    protected void putPostActions(Transaction transaction,
                                  ByteArray key,
                                  List<byte[]> deletedVals,
                                  List<byte[]> remainingVals,
                                  Versioned<byte[]> value,
                                  byte[] transforms) {
        for(byte[] rawVal: deletedVals) {
            secIndexRemove(transaction,
                           key,
                           secIdxProcessor.extractSecondaryValues(getVersionedValue(rawVal).getValue()));
        }

        for(byte[] rawVal: remainingVals) {
            secIndexAdd(transaction,
                        key,
                        secIdxProcessor.extractSecondaryValues(getVersionedValue(rawVal).getValue()));
        }

        secIndexAdd(transaction, key, secIdxProcessor.extractSecondaryValues(value.getValue()));
    }

    public static class CompositeKeyHandler implements Comparator<byte[]> {

        private static void writeInt(byte[] array, int offset, int v) {
            array[offset] = (byte) ((v >>> 24) & 0xFF);
            array[offset + 1] = (byte) ((v >>> 16) & 0xFF);
            array[offset + 2] = (byte) ((v >>> 8) & 0xFF);
            array[offset + 3] = (byte) ((v >>> 0) & 0xFF);
        }

        private static int readInt(byte[] array, int offset) {
            int ch1 = array[offset];
            int ch2 = array[offset + 1];
            int ch3 = array[offset + 2];
            int ch4 = array[offset + 3];
            return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
        }

        private static int compare(byte[] key1,
                                   int offset1,
                                   int a1Len,
                                   byte[] key2,
                                   int offset2,
                                   int a2Len) {
            int limit = Math.min(a1Len, a2Len);
            for(int i = 0; i < limit; i++) {
                byte b1 = key1[offset1 + i];
                byte b2 = key2[offset2 + i];
                if(b1 == b2) {
                    continue;
                } else {
                    // Remember, bytes are signed, so convert to shorts so that
                    // we effectively do an unsigned byte comparison.
                    return (b1 & 0xff) - (b2 & 0xff);
                }
            }
            return (a1Len - a2Len);
        }

        /** Compares both secondary and primary key, to make it unique */
        public int compare(byte[] cKey1, byte[] cKey2) {
            int result = compare(cKey1, 4, cKey1.length - 4, cKey2, 4, cKey2.length - 4);
            return result;
        }

        /** Compares secondary keys within the given composite keys */
        public static int compareSecondaryKeys(byte[] cKey1, byte[] cKey2) {
            int value1Size = readInt(cKey1, 0);
            int value2Size = readInt(cKey2, 0);
            return compare(cKey1, 4, value1Size, cKey2, 4, value2Size);
        }

        public static byte[] createCompositeKey(byte[] sk, byte[] pk) {
            byte[] result = new byte[sk.length + pk.length + 4];
            writeInt(result, 0, sk.length);
            System.arraycopy(sk, 0, result, 4, sk.length);
            System.arraycopy(pk, 0, result, 4 + sk.length, pk.length);
            return result;
        }

        public static byte[] getPrimaryKey(byte[] compositeKey) {
            int secKeySize = readInt(compositeKey, 0);
            int primaryKeySize = compositeKey.length - secKeySize - 4;
            byte[] result = new byte[primaryKeySize];
            System.arraycopy(compositeKey, 4 + secKeySize, result, 0, result.length);
            return result;
        }

    }

    private final static DatabaseEntry EMPTY_VALUE = new DatabaseEntry(ArrayUtils.EMPTY_BYTE_ARRAY);
    private final static DatabaseEntry PARTIAL_ENTRY = new DatabaseEntry(ArrayUtils.EMPTY_BYTE_ARRAY);
    static {
        PARTIAL_ENTRY.setPartial(0, 0, true);
    }

    private void secIndexAdd(Transaction tx, ByteArray key, Map<String, byte[]> values) {
        for(Entry<String, byte[]> entry: values.entrySet()) {
            String fieldName = entry.getKey();
            byte[] fieldValue = entry.getValue();

            Cursor cursor = secDbsByField.get(fieldName).openCursor(tx, null);
            try {
                DatabaseEntry keyEntry = new DatabaseEntry(CompositeKeyHandler.createCompositeKey(fieldValue,
                                                                                                  key.get()));
                OperationStatus status = cursor.put(keyEntry, EMPTY_VALUE);
                if(logger.isTraceEnabled())
                    logger.trace("Putting sec idx " + fieldName + ". key: " + keyEntry);
                if(status != OperationStatus.SUCCESS)
                    throw new PersistenceFailureException("Put operation failed with status: "
                                                          + status);
            } finally {
                cursor.close();
            }
        }
    }

    private void secIndexRemove(Transaction tx, ByteArray key, Map<String, byte[]> values) {
        for(Entry<String, byte[]> entry: values.entrySet()) {
            String fieldName = entry.getKey();

            Cursor cursor = secDbsByField.get(fieldName).openCursor(tx, null);
            try {
                DatabaseEntry keyEntry = new DatabaseEntry(CompositeKeyHandler.createCompositeKey(entry.getValue(),
                                                                                                  key.get()));
                OperationStatus status = cursor.getSearchKey(keyEntry, PARTIAL_ENTRY, null);
                if(logger.isTraceEnabled())
                    logger.trace("Removing sec idx " + fieldName + ". key: " + keyEntry);
                if(status == OperationStatus.NOTFOUND)
                    continue;
                if(status != OperationStatus.SUCCESS)
                    throw new PersistenceFailureException("Search operation failed with status: "
                                                          + status);
                status = cursor.delete();
                if(status != OperationStatus.SUCCESS)
                    throw new PersistenceFailureException("Delete operation failed with status: "
                                                          + status);
            } finally {
                cursor.close();
            }
        }
    }

    @Override
    protected boolean truncatePostActions(Transaction tx) {
        for(String secDbName: secDbNamesByField.values()) {
            environment.truncateDatabase(tx, secDbName, false);
        }
        return true;
    }

    @Override
    protected void deletePostActions(Transaction transaction,
                                     ByteArray key,
                                     List<byte[]> deletedVals,
                                     List<byte[]> remainingVals) {
        for(byte[] rawVal: deletedVals) {
            secIndexRemove(transaction,
                           key,
                           secIdxProcessor.extractSecondaryValues(getVersionedValue(rawVal).getValue()));
        }

        for(byte[] rawVal: remainingVals) {
            secIndexAdd(transaction,
                        key,
                        secIdxProcessor.extractSecondaryValues(getVersionedValue(rawVal).getValue()));
        }
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case SECONDARY_INDEX:
                return true;
            default:
                throw new NoSuchCapabilityException(capability, getName());
        }
    }

}
