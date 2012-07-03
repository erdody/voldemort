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
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;

import com.google.protobuf.Message;

public class FetchKeysForQueryRequestHandler implements StreamRequestHandler {

    protected final Logger logger = Logger.getLogger(getClass());

    private final FetchKeysForQueryRequest request;

    protected final ErrorCodeMapper errorCodeMapper;

    protected final StorageEngine<ByteArray, byte[], byte[]> storageEngine;

    protected final ClosableIterator<KeyMatch<ByteArray>> keyMatchIterator;

    protected long counter;

    protected final long startTime;

    // protected final Handle handle;

    // protected final EventThrottler throttler;

    // protected final StreamStats stats;

    protected int nodeId;

    protected StoreDefinition storeDef;

    public FetchKeysForQueryRequestHandler(FetchKeysForQueryRequest request,
                                           StoreRepository storeRepository,
                                           ErrorCodeMapper errorCodeMapper) {
        this.request = request;
        this.storageEngine = AdminServiceRequestHandler.getStorageEngine(storeRepository,
                                                                         request.getStore());
        this.keyMatchIterator = storageEngine.keys(request.getQuery());
        this.startTime = System.currentTimeMillis();
        this.errorCodeMapper = errorCodeMapper;
    }

    public final StreamRequestDirection getDirection() {
        return StreamRequestDirection.WRITING;
    }

    public final void close(DataOutputStream outputStream) throws IOException {
        logger.info("Successfully returned " + counter + " keys for store '"
                    + storageEngine.getName() + "' in "
                    + ((System.currentTimeMillis() - startTime) / 1000) + " s");

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

        // long startNs = System.nanoTime();
        KeyMatch<ByteArray> keyMatch = keyMatchIterator.next();
        // stats.recordDiskTime(handle, System.nanoTime() - startNs);

        // throttler.maybeThrottle(key.length());

        {
            VAdminProto.FetchKeysForQueryResponse.Builder response = VAdminProto.FetchKeysForQueryResponse.newBuilder();

            // TODO encode the COMPLETE match
            response.setKey(ProtoUtils.encodeBytes(keyMatch.getKey()));

            // handle.incrementEntriesScanned();
            Message message = response.build();

            // startNs = System.nanoTime();
            ProtoUtils.writeMessage(outputStream, message);
            // stats.recordNetworkTime(handle, System.nanoTime() - startNs);
        }

        // log progress
        counter++;

        if(0 == counter % 100000) {
            long totalTime = (System.currentTimeMillis() - startTime) / 1000;

            logger.info("Fetch keys for query returned " + counter + " keys for store '"
                        + storageEngine.getName() + " in " + totalTime + " s");
        }

        if(keyMatchIterator.hasNext())
            return StreamRequestHandlerState.WRITING;
        else {
            // stats.closeHandle(handle);
            return StreamRequestHandlerState.COMPLETE;
        }
    }
}
