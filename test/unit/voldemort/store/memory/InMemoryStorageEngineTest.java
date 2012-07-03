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

package voldemort.store.memory;

import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

public class InMemoryStorageEngineTest extends AbstractStorageEngineTest {

    private StorageEngine<ByteArray, byte[], byte[]> store;

    public StorageEngine<ByteArray, byte[], byte[]> getStorageEngine() {
        return store;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.store = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test");
    }

}
