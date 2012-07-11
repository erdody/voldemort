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

package voldemort.store;

import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Sets;

/**
 * A base storage class which is actually responsible for data persistence. This
 * interface implies all the usual responsibilities of a Store implementation,
 * and in addition
 * <ol>
 * <li>The implementation MUST throw an ObsoleteVersionException if the user
 * attempts to put a version which is strictly before an existing version
 * (concurrent is okay)</li>
 * <li>The implementation MUST increment this version number when the value is
 * stored.</li>
 * <li>The implementation MUST contain an ID identifying it as part of the
 * cluster</li>
 * </ol>
 * 
 * A hash value can be produced for known subtrees of a StorageEngine
 * 
 * 
 * @param <K> The type of the key being stored
 * @param <V> The type of the value being stored
 * @param <T> The type of the transforms
 * 
 */
public interface StorageEngine<K, V, T> extends Store<K, V, T> {

    /**
     * Get an iterator over pairs of entries in the store. The key is the first
     * element in the pair and the versioned value is the second element.
     * 
     * Note that the iterator need not be threadsafe, and that it must be
     * manually closed after use.
     * 
     * @return An iterator over the entries in this StorageEngine.
     */
    public ClosableIterator<Pair<K, Versioned<V>>> entries();

    /**
     * Get an iterator over keys in the store.
     * 
     * Note that the iterator need not be threadsafe, and that it must be
     * manually closed after use.
     * 
     * @return An iterator over the keys in this StorageEngine.
     */
    public ClosableIterator<K> keys();

    /**
     * Truncate all entries in the store
     */
    public void truncate();

    /**
     * Is the data persistence aware of partitions? In other words is the data
     * internally stored on a per partition basis or together
     * 
     * @return Boolean indicating if the data persistence is partition aware
     */
    public boolean isPartitionAware();

    /**
     * Retrieves all the keys that comply with the given query.
     * 
     * <p>
     * Note that the iterator need not be threadsafe, and that it must be
     * manually closed after use.
     * 
     * @return set of matching keys.
     */
    public ClosableIterator<KeyMatch<K>> keys(String query);

    /**
     * A match for a secondary index query. This match contains the key that
     * matched and the list of Versions that matched the given query (always
     * non-empty) and the list of Versions that didn't match.
     * <p>
     * We need to have both lists because inconsistency resolution (check what
     * version is the latest one) is done at the client side.
     */
    public class KeyMatch<K> {

        private final K key;

        private final List<? extends Version> matchingVersions;

        private final List<? extends Version> unmatchingVersions;

        public KeyMatch(K key,
                        List<? extends Version> matchingVersions,
                        List<? extends Version> unmatchingVersions) {
            this.key = key;
            this.matchingVersions = matchingVersions;
            this.unmatchingVersions = unmatchingVersions;
        }

        /** @return key that has at least one match */
        public K getKey() {
            return key;
        }

        /** @return list of versions of this key that matched */
        public List<? extends Version> getMatchingVersions() {
            return matchingVersions;
        }

        /** @return list of versions of this key that didn't match */
        public List<? extends Version> getUnmatchingVersions() {
            return unmatchingVersions;
        }

        /**
         * Implementation note: This should be needed for testing only, it
         * compares versions with no special order.
         */
        @Override
        public boolean equals(Object obj) {
            if(obj == null) {
                return false;
            }
            if(obj == this) {
                return true;
            }
            if(obj.getClass() != getClass()) {
                return false;
            }
            @SuppressWarnings("unchecked")
            KeyMatch<K> rhs = (KeyMatch<K>) obj;
            return new EqualsBuilder().append(key, rhs.key)
                                      .append(Sets.newHashSet(matchingVersions),
                                              Sets.newHashSet(rhs.matchingVersions))
                                      .append(Sets.newHashSet(unmatchingVersions),
                                              Sets.newHashSet(rhs.unmatchingVersions))
                                      .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(key)
                                        .append(Sets.newHashSet(matchingVersions))
                                        .append(Sets.newHashSet(unmatchingVersions))
                                        .toHashCode();
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }

    }
}
