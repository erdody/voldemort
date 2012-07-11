package voldemort.store.bdb;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import surver.pub.expression.Expression;
import surver.pub.expression.ExpressionParser;
import voldemort.annotations.Experimental;
import voldemort.secondary.SecondaryIndexProcessor;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StoreCapabilityType;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.ForwardCursor;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * {@link BdbStorageEngine} extension that provides secondary index support.
 */
@Experimental
public class BdbStorageEngineSI extends BdbStorageEngine {

    private final SecondaryIndexProcessor secIdxProcessor;

    public BdbStorageEngineSI(String name,
                              Environment environment,
                              Database database,
                              BdbRuntimeConfig runtimeConfig,
                              SecondaryIndexProcessor secIdxProcessor) {
        super(name, environment, database, runtimeConfig);
        this.secIdxProcessor = secIdxProcessor;
    }

    @Override
    public ClosableIterator<KeyMatch<ByteArray>> keys(String query) {
        Cursor cursor = getBdbDatabase().openCursor(null, null);
        return new BdbFilteredKeysIterator(cursor, query);
    }

    /**
     * Iterator that uses a query to decide which keys should be returned.
     * <p>
     * Since there could be many versions for a given key, and
     */
    private class BdbFilteredKeysIterator extends BdbIterator<KeyMatch<ByteArray>> {

        private Expression condition;

        public BdbFilteredKeysIterator(ForwardCursor cursor, String query) {
            super(cursor, true);
            condition = ExpressionParser.parse(query, secIdxProcessor.getSecondaryFields());
        }

        private byte[] lastKey = null;
        private KeyMatch<ByteArray> currentMatch = null;
        private OperationStatus lastStatus = null;

        @Override
        protected KeyMatch<ByteArray> get(DatabaseEntry key, DatabaseEntry value) {
            return currentMatch;
        }

        @Override
        protected OperationStatus moveCursor(DatabaseEntry key, DatabaseEntry value)
                throws DatabaseException {
            currentMatch = null;

            if(lastKey == null) {
                OperationStatus status = cursor.getNext(key, value, LockMode.READ_UNCOMMITTED);
                if(status != OperationStatus.SUCCESS)
                    return status;
                lastKey = key.getData();
            } else {
                if(lastStatus != OperationStatus.SUCCESS)
                    return lastStatus;
            }

            do {
                lastStatus = cursor.getNext(key, value, LockMode.READ_UNCOMMITTED);
                if(lastStatus == OperationStatus.SUCCESS) {
                    boolean multipleVersions = keyHandler.compareOriginalKeyOnly(lastKey,
                                                                                 key.getData()) == 0;

                    if(multipleVersions) {
                        ListMultimap<Boolean, byte[]> partitioned = ArrayListMultimap.create();

                        partitioned.put(match(lastKey), lastKey);

                        do {
                            lastKey = key.getData();
                            partitioned.put(match(lastKey), lastKey);
                            lastStatus = cursor.getNext(key, value, LockMode.READ_UNCOMMITTED);
                        } while(lastStatus == OperationStatus.SUCCESS
                                && keyHandler.compareOriginalKeyOnly(lastKey, key.getData()) == 0);

                        // if any version match
                        if(!partitioned.get(true).isEmpty()) {
                            currentMatch = new KeyMatch<ByteArray>(new ByteArray(keyHandler.getOriginalKey(lastKey)),
                                                                   toVersion(partitioned.get(true)),
                                                                   toVersion(partitioned.get(false)));
                        }

                        if(lastStatus == OperationStatus.SUCCESS)
                            lastKey = key.getData();
                    } else {
                        processSingleVersion();
                        lastKey = key.getData();
                    }
                }
            } while(currentMatch == null && lastStatus == OperationStatus.SUCCESS);

            if(currentMatch == null)
                processSingleVersion();

            return OperationStatus.SUCCESS;
        }

        private List<Version> toVersion(List<byte[]> keys) {
            return Lists.transform(keys, new Function<byte[], Version>() {

                public Version apply(byte[] key) {
                    return VersionedKeyHandler.getVectorClock(key);
                }

            });
        }

        private void processSingleVersion() {
            if(match(lastKey)) {
                currentMatch = new KeyMatch<ByteArray>(new ByteArray(keyHandler.getOriginalKey(lastKey)),
                                                       Collections.<Version> singletonList(VersionedKeyHandler.getVectorClock(lastKey)),
                                                       Collections.<Version> emptyList());
            }
        }

        private boolean match(byte[] cKey) {
            byte[] secIdxValues = getSIKeyHandler().getSecIdxBytes(cKey);
            return condition.evaluate(secIdxProcessor.parseSecondaryValues(secIdxValues));
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

    @Override
    protected SIVersionedKeyHandler createKeyHandler() {
        return new SIVersionedKeyHandler();
    }

    private SIVersionedKeyHandler getSIKeyHandler() {
        return (SIVersionedKeyHandler) keyHandler;
    }

    /**
     * Composite key handler that appends secondary index values to the payload
     * segment.
     */
    public static class SIVersionedKeyHandler extends VersionedKeyHandler {

        private static final byte[] EMPTY = serializeSize((short) 0);

        @Override
        protected byte[] getEmptyPayload() {
            return ByteUtils.cat(super.getEmptyPayload(), EMPTY);
        }

        @Override
        protected int getPayloadSize(byte[] cKey) {
            int superSize = super.getPayloadSize(cKey);
            return ByteBuffer.wrap(cKey, superSize, 2).getShort() + 2 + superSize;
        }

        /** Converts the given size into a byte array */
        private static byte[] serializeSize(short size) {
            return ByteBuffer.allocate(2).putShort(size).array();
        }

        /**
         * @return serialized secondary index values block from the given
         *         composite key
         */
        public byte[] getSecIdxBytes(byte[] cKey) {
            int superSize = super.getPayloadSize(cKey);
            int secIdxSize = ByteBuffer.wrap(cKey, superSize, 2).getShort();
            int secIdxPos = superSize + 2;
            return ByteUtils.copy(cKey, secIdxPos, secIdxPos + secIdxSize);
        }
    }

    @Override
    protected byte[] assembleKeyPayload(ByteArray key, Versioned<byte[]> value) {
        byte[] secIdxBlock = secIdxProcessor.extractSecondaryValues(value.getValue());
        return ByteUtils.cat(super.assembleKeyPayload(key, value),
                             SIVersionedKeyHandler.serializeSize((short) secIdxBlock.length),
                             secIdxBlock);
    }

}
