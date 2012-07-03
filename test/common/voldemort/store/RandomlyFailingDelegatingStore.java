package voldemort.store;

import voldemort.VoldemortException;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

public class RandomlyFailingDelegatingStore<K, V, T> extends DelegatingStore<K, V, T> implements
        StorageEngine<K, V, T> {

    private static double FAIL_PROBABILITY = 0.60;
    private final StorageEngine<K, V, T> innerStorageEngine;

    public RandomlyFailingDelegatingStore(StorageEngine<K, V, T> innerStorageEngine) {
        super(innerStorageEngine);
        this.innerStorageEngine = innerStorageEngine;
    }

    static class FailingIterator<T> implements ClosableIterator<T> {

        private final ClosableIterator<T> iterator;

        FailingIterator(ClosableIterator<T> iterator) {
            this.iterator = iterator;
        }

        public void close() {
            iterator.close();
        }

        public boolean hasNext() {
            return iterator.hasNext();
        }

        public T next() {
            if(Math.random() > FAIL_PROBABILITY)
                return iterator.next();

            throw new VoldemortException("Failing now !!");
        }

        public void remove() {}
    }

    public ClosableIterator<Pair<K, Versioned<V>>> entries() {
        return new FailingIterator<Pair<K, Versioned<V>>>(innerStorageEngine.entries());
    }

    public ClosableIterator<K> keys() {
        return new FailingIterator<K>(innerStorageEngine.keys());
    }

    public ClosableIterator<KeyMatch<K>> keys(String query) {
        return new FailingIterator<KeyMatch<K>>(innerStorageEngine.keys(query));
    }

    public void truncate() {
        if(Math.random() > FAIL_PROBABILITY) {
            innerStorageEngine.truncate();
        }

        throw new VoldemortException("Failing now !!");
    }

    public boolean isPartitionAware() {
        return innerStorageEngine.isPartitionAware();
    }
}