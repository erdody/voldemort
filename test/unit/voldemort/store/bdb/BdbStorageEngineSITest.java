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

import java.util.Comparator;

import voldemort.secondary.SecondaryIndexTestUtils;

/** Unit test for {@link BdbStorageEngineSI} */
public class BdbStorageEngineSITest extends BdbStorageEngineTest {

    @Override
    protected BdbStorageEngine createBdbStorageEngine(BdbRuntimeConfig runtimeConfig) {
        return new BdbStorageEngineSI("test",
                                      database.getEnvironment(),
                                      database,
                                      runtimeConfig,
                                      SecondaryIndexTestUtils.getSecIdxProcessor());
    }

    @Override
    protected Class<? extends Comparator<byte[]>> getBtreeComparator() {
        return BdbStorageEngineSI.SIVersionedKeyHandler.class;
    }

    @Override
    protected boolean isSecondaryIndexEnabled() {
        return true;
    }

}
