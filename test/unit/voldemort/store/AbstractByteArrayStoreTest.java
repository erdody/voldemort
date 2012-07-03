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

import org.junit.Test;

import voldemort.TestUtils;
import voldemort.secondary.SecondaryIndexTestUtils;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public abstract class AbstractByteArrayStoreTest extends
        AbstractStoreTest<ByteArray, byte[], byte[]> {

    @Override
    public List<ByteArray> getKeys(int numValues) {
        List<ByteArray> keys = Lists.newArrayList();
        for(byte[] array: this.getByteValues(numValues, 8))
            keys.add(new ByteArray(array));
        return keys;
    }

    @Override
    protected boolean valuesEqual(byte[] t1, byte[] t2) {
        return TestUtils.bytesEqual(t1, t2);
    }

    @Test
    public void testEmptyByteArray() throws Exception {
        if(isSecondaryIndexEnabled())
            return;

        Store<ByteArray, byte[], byte[]> store = getStore();
        Versioned<byte[]> bytes = new Versioned<byte[]>(new byte[0]);
        store.put(new ByteArray(new byte[0]), bytes, null);
        List<Versioned<byte[]>> found = store.get(new ByteArray(new byte[0]), null);
        assertEquals("Incorrect number of results.", 1, found.size());
        assertEquals("Get doesn't equal put.", bytes, found.get(0));
    }

    /**
     * @return true if secondary index tests should be performed. Disabled by
     *         default.
     */
    protected boolean isSecondaryIndexEnabled() {
        return false;
    }

    @Override
    public ByteArray getKey(int size) {
        return new ByteArray(TestUtils.randomBytes(size));
    }

    @Override
    public List<byte[]> getValues(int numValues) {
        return SecondaryIndexTestUtils.getValues(numValues);
    }

    @Override
    public byte[] getValue(int size) {
        return SecondaryIndexTestUtils.getValue(size);
    }

}
