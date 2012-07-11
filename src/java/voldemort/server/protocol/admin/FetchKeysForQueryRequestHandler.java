package voldemort.server.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.FetchKeysForQueryRequest;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.StorageEngine.KeyMatch;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;

/**
 * Request Handler for secondary queries (key iterator based on a query).
 * <p>
 * For now we only transfer the matching keys, but if we want to implement a
 * multi-node query we would need to send the complete set of versions for each
 * key. See {@link #handleRequest(DataInputStream, DataOutputStream)}
 */
public class FetchKeysForQueryRequestHandler implements StreamRequestHandler {

    private final Logger logger = Logger.getLogger(getClass());

    private final FetchKeysForQueryRequest request;

    private final ErrorCodeMapper errorCodeMapper;

    private final StorageEngine<ByteArray, byte[], byte[]> storageEngine;

    private final ClosableIterator<KeyMatch<ByteArray>> keyMatchIterator;

    private final long startMillis;

    private long counter;

    public FetchKeysForQueryRequestHandler(FetchKeysForQueryRequest request,
                                           StoreRepository storeRepository,
                                           ErrorCodeMapper errorCodeMapper) {
        this.request = request;
        this.storageEngine = AdminServiceRequestHandler.getStorageEngine(storeRepository,
                                                                         request.getStore());
        this.keyMatchIterator = storageEngine.keys(request.getQuery());
        this.startMillis = System.currentTimeMillis();
        this.errorCodeMapper = errorCodeMapper;
    }

    public final StreamRequestDirection getDirection() {
        return StreamRequestDirection.WRITING;
    }

    public final void close(DataOutputStream outputStream) throws IOException {
        logger.info("Successfully returned " + counter + " keys for store '"
                    + storageEngine.getName() + "' in "
                    + ((System.currentTimeMillis() - startMillis) / 1000) + " s");

        if(keyMatchIterator != null)
            keyMatchIterator.close();

        ProtoUtils.writeEndOfStream(outputStream);
    }

    public final void handleError(DataOutputStream outputStream, VoldemortException e)
            throws IOException {
        VAdminProto.FetchPartitionEntriesResponse response = VAdminProto.FetchPartitionEntriesResponse.newBuilder()
                                                                                                      .setError(ProtoUtils.encodeError(errorCodeMapper,
                                                                                                                                       e))
                                                                                                      .build();

        ProtoUtils.writeMessage(outputStream, response);
        logger.error(this.getClass().getName() + " failed for request(" + request.toString() + ")",
                     e);
    }

    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException {
        if(!keyMatchIterator.hasNext())
            return StreamRequestHandlerState.COMPLETE;

        KeyMatch<ByteArray> keyMatch = keyMatchIterator.next();

        {
            VAdminProto.FetchKeysForQueryResponse.Builder response = VAdminProto.FetchKeysForQueryResponse.newBuilder();
            response.setKey(ProtoUtils.encodeBytes(keyMatch.getKey()));
            ProtoUtils.writeMessage(outputStream, response.build());
        }

        // log progress
        counter++;
        if(counter % 100000 == 0) {
            long totalTime = (System.currentTimeMillis() - startMillis) / 1000;

            logger.info("Fetch keys for query returned " + counter + " keys for store '"
                        + storageEngine.getName() + " in " + totalTime + " s");
        }

        return keyMatchIterator.hasNext() ? StreamRequestHandlerState.WRITING
                                         : StreamRequestHandlerState.COMPLETE;
    }
}
