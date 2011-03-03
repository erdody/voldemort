package voldemort.store.bdb;

import java.util.List;

import voldemort.secondary.SecondaryIndexProcessor;
import voldemort.secondary.SecondaryIndexProcessorFactory;
import voldemort.secondary.SecondaryIndexSupported;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.SerializerFactory;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.views.ViewStorageConfiguration;
import voldemort.utils.ByteArray;

import com.sleepycat.je.Database;
import com.sleepycat.je.Environment;

/** {@link StorageConfiguration} for {@link BdbStorageEngineSI} */
@SecondaryIndexSupported
public class BdbStorageConfigurationSI extends BdbStorageConfiguration {

    public static final String TYPE_NAME_SI = "bdb_si";

    private List<StoreDefinition> storeDefs;

    public BdbStorageConfigurationSI(VoldemortConfig config, List<StoreDefinition> stores) {
        super(config);
        this.storeDefs = stores;
    }

    @Override
    protected StorageEngine<ByteArray, byte[], byte[]> createStore(String storeName,
                                                                   Environment environment,
                                                                   Database db,
                                                                   boolean cursorPreload) {
        StoreDefinition storeDef = StoreUtils.getStoreDef(storeDefs, storeName);
        String factoryName = storeDef.getSerializerFactory();
        SerializerFactory factory = factoryName == null ? new DefaultSerializerFactory()
                                                       : ViewStorageConfiguration.loadSerializerFactory(factoryName);

        SecondaryIndexProcessor secIdxProcessor = SecondaryIndexProcessorFactory.getProcessor(factory,
                                                                                              storeDef.getSecondaryIndexDefinitions(),
                                                                                              storeDef.getValueSerializer());

        return new BdbStorageEngineSI(storeName, environment, db, cursorPreload, secIdxProcessor);
    }

    @Override
    public String getType() {
        return TYPE_NAME_SI;
    }

}
