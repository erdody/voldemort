package voldemort.store.bdb;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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

    private Map<String, Database> secDbsByField = Maps.newHashMap();
    private Map<String, String> secDbNamesByField = Maps.newHashMap();
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
        secConfig.setSortedDuplicates(true);

        for(String fieldName: secIdxProcessor.getSecondaryFields()) {
            String secDbName = getSecDbName(database, fieldName);
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
        private final Comparator<byte[]> comp;

        public BdbKeysRangeIterator(Cursor cursor, byte[] from, byte[] to) {
            super(cursor, false);
            this.from = from;
            this.to = to;
            this.comp = cursor.getDatabase().getConfig().getBtreeComparator();
        }

        @Override
        protected ByteArray get(DatabaseEntry key, DatabaseEntry value) {
            return new ByteArray(value.getData());
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

            if(opStatus == OperationStatus.SUCCESS && compareValues(key.getData(), to) > 0) {
                return OperationStatus.NOTFOUND;
            }
            return opStatus;
        }

        private int compareValues(byte[] value, byte[] limit) {
            if(comp != null)
                return comp.compare(value, limit);
            else
                return ByteArray.compare(value, limit);
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

    private void secIndexAdd(Transaction tx, ByteArray key, Map<String, byte[]> values) {
        DatabaseEntry valueEntry = new DatabaseEntry(key.get());
        for(Entry<String, byte[]> entry: values.entrySet()) {
            String fieldName = entry.getKey();

            Cursor cursor = secDbsByField.get(fieldName).openCursor(tx, null);
            try {
                DatabaseEntry keyEntry = new DatabaseEntry(entry.getValue());
                OperationStatus status = cursor.put(keyEntry, valueEntry);
                if(logger.isTraceEnabled())
                    logger.trace("Putting sec idx " + fieldName + ". key: " + keyEntry + " value "
                                 + valueEntry);
                if(status != OperationStatus.SUCCESS)
                    throw new PersistenceFailureException("Put operation failed with status: "
                                                          + status);
            } finally {
                cursor.close();
            }
        }
    }

    private void secIndexRemove(Transaction tx, ByteArray key, Map<String, byte[]> values) {
        DatabaseEntry valueEntry = new DatabaseEntry(key.get());
        for(Entry<String, byte[]> entry: values.entrySet()) {
            String fieldName = entry.getKey();

            Cursor cursor = secDbsByField.get(fieldName).openCursor(tx, null);
            try {
                DatabaseEntry keyEntry = new DatabaseEntry(entry.getValue());
                OperationStatus status = cursor.getSearchBoth(keyEntry, valueEntry, null);
                if(logger.isTraceEnabled())
                    logger.trace("Removing sec idx " + fieldName + ". key: " + keyEntry + " value "
                                 + valueEntry);
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
