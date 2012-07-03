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
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A store that always throws an exception for every operation
 * 
 * 
 */
public class FailingStore<K, V, T> implements Store<K, V, T> {

    private final String name;
    private final Class<? extends VoldemortException> exceptionClass;

    public FailingStore(String name) {
        this(name, VoldemortException.class);
    }

    /**
     * Create a new failing store that throws a new exception of the given class
     * for every operation
     * 
     * @param name Store name
     * @param exceptionClass Class to throw for every operation. Needs to have a
     *        String constructor.
     */
    public FailingStore(String name, Class<? extends VoldemortException> exceptionClass) {
        this.name = Utils.notNull(name);
        this.exceptionClass = exceptionClass;
    }

    public void close() throws VoldemortException {
        throw getException();
    }

    public List<Versioned<V>> get(K key, T transforms) throws VoldemortException {
        throw getException();
    }

    public String getName() {
        return name;
    }

    public boolean delete(K key, Version value) throws VoldemortException {
        throw getException();
    }

    public void put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        throw getException();
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        throw getException();
    }

    public java.util.List<Version> getVersions(K key) {
        throw getException();
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    private VoldemortException getException() {
        try {
            return exceptionClass.getConstructor(String.class)
                                 .newInstance("Simulated operation failure");
        } catch(Exception e) {
            return new VoldemortException("Could not simulate failure", e);
        }
    }
}
