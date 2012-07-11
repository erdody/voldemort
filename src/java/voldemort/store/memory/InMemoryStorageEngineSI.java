package voldemort.store.memory;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import surver.pub.expression.Expression;
import surver.pub.expression.ExpressionParser;
import voldemort.VoldemortException;
import voldemort.secondary.SecondaryIndexProcessor;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.base.Function;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;

/**
 * An InMemoryStorageEngine extension that adds secondary index support.
 * 
 */
public class InMemoryStorageEngineSI extends InMemoryStorageEngine<ByteArray, byte[], byte[]> {

    // Secondary index blocks, by <key, version>
    private final ConcurrentMap<Pair<ByteArray, Version>, byte[]> secIdxValues = new ConcurrentHashMap<Pair<ByteArray, Version>, byte[]>();

    private final SecondaryIndexProcessor secIdxProcessor;

    public InMemoryStorageEngineSI(String name, SecondaryIndexProcessor secIdxProcessor) {
        this(name, new ConcurrentHashMap<ByteArray, List<Versioned<byte[]>>>(), secIdxProcessor);
    }

    public InMemoryStorageEngineSI(String name,
                                   ConcurrentMap<ByteArray, List<Versioned<byte[]>>> map,
                                   SecondaryIndexProcessor secIdxProcessor) {
        super(name, map);
        this.secIdxProcessor = secIdxProcessor;
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        super.put(key, value, transforms);
        secIdxValues.put(Pair.create(key, value.getVersion()),
                         secIdxProcessor.extractSecondaryValues(value.getValue()));
    }

    @Override
    public boolean delete(ByteArray key, Version version) {
        secIdxValues.remove(Pair.create(key, version));
        return super.delete(key, version);
    }

    /**
     * @return next key that has at least one version that matches the given
     *         condition. Null if none found.
     */
    private KeyMatch<ByteArray> getNextMatch(final Expression condition,
                                             Iterator<Entry<ByteArray, List<Versioned<byte[]>>>> entriesIterator) {
        while(entriesIterator.hasNext()) {
            final Entry<ByteArray, List<Versioned<byte[]>>> entry = entriesIterator.next();

            List<Version> versions = Lists.transform(entry.getValue(),
                                                     new Function<Versioned<byte[]>, Version>() {

                                                         public Version apply(Versioned<byte[]> versioned) {
                                                             return versioned.getVersion();
                                                         }
                                                     });

            // index versions by match (true has the list of matches)
            ListMultimap<Boolean, Version> index = Multimaps.index(versions,
                                                                   new Function<Version, Boolean>() {

                                                                       public Boolean apply(Version version) {
                                                                           return condition.evaluate(secIdxProcessor.parseSecondaryValues(secIdxValues.get(Pair.create(entry.getKey(),
                                                                                                                                                                 version))));
                                                                       }

                                                                   });

            // if a match was found
            if(!index.get(true).isEmpty())
                return new KeyMatch<ByteArray>(entry.getKey(), index.get(true), index.get(false));
        }
        return null;
    }

    @Override
    public ClosableIterator<KeyMatch<ByteArray>> keys(String query) {
        final Expression condition = ExpressionParser.parse(query,
                                                            secIdxProcessor.getSecondaryFields());

        final Iterator<Entry<ByteArray, List<Versioned<byte[]>>>> iterator = map.entrySet()
                                                                                .iterator();

        return new ClosableIterator<KeyMatch<ByteArray>>() {

            private KeyMatch<ByteArray> currentMatch = getNextMatch(condition, iterator);

            public boolean hasNext() {
                return currentMatch != null;
            }

            public KeyMatch<ByteArray> next() {
                KeyMatch<ByteArray> result = currentMatch;
                if(result != null)
                    currentMatch = getNextMatch(condition, iterator);
                return result;
            }

            public void remove() {
                throw new UnsupportedOperationException("No removal y'all.");
            }

            public void close() {}

        };
    }
}
