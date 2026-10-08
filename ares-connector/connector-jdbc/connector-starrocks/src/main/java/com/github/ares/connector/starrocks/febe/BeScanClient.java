package com.github.ares.connector.starrocks.febe;

import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.AresException;
import com.starrocks.shade.org.apache.thrift.TException;
import com.starrocks.shade.org.apache.thrift.protocol.TBinaryProtocol;
import com.starrocks.shade.org.apache.thrift.transport.TSocket;
import com.starrocks.thrift.TScanBatchResult;
import com.starrocks.thrift.TScanCloseParams;
import com.starrocks.thrift.TScanNextBatchParams;
import com.starrocks.thrift.TScanOpenParams;
import com.starrocks.thrift.TScanOpenResult;
import com.starrocks.thrift.TStarrocksExternalService;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Opens one tablet scanner on a BE thrift port and reads Arrow batches. */
final class BeScanClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BeScanClient.class);
    private static final String DEFAULT_CLUSTER = "default_cluster";

    private final StarRocksSplit split;
    private final StarRocksFeBeConfig config;
    private final AresRowType rowType;
    private final BufferAllocator allocator;
    private TSocket socket;
    private TStarrocksExternalService.Client client;
    private String contextId;
    private long offset;
    private boolean eos;

    BeScanClient(
            StarRocksSplit split,
            StarRocksFeBeConfig config,
            AresRowType rowType,
            BufferAllocator allocator) {
        this.split = split;
        this.config = config;
        this.rowType = rowType;
        this.allocator = allocator;
    }

    void open() {
        String[] hostPort = split.getBeAddress().split(":");
        if (hostPort.length != 2) {
            throw new AresException(
                    "StarRocks BE address must be host:port, got " + split.getBeAddress());
        }
        String host = hostPort[0].trim();
        int port = Integer.parseInt(hostPort[1].trim());
        socket =
                new TSocket(
                        host,
                        port,
                        config.getScanConnectTimeoutMs(),
                        config.getScanConnectTimeoutMs());
        try {
            socket.open();
            client = new TStarrocksExternalService.Client(new TBinaryProtocol(socket));
            TScanOpenParams params = new TScanOpenParams();
            params.setTablet_ids(new ArrayList<>(split.getTabletIds()));
            params.setOpaqued_query_plan(split.getOpaqueQueryPlan());
            params.setCluster(DEFAULT_CLUSTER);
            params.setDatabase(split.getDatabase());
            params.setTable(split.getTable());
            params.setUser(config.getUsername());
            params.setPasswd(config.getPassword());
            params.setBatch_size(config.getScanBatchRows());
            params.setKeep_alive_min(
                    (short) Math.min(Short.MAX_VALUE, config.getScanKeepAliveMin()));
            params.setQuery_timeout(config.getScanQueryTimeoutSec());
            params.setMem_limit(config.getScanMemLimit());
            TScanOpenResult result = client.open_scanner(params);
            assertOk(result.getStatus(), "open scanner on " + split.getBeAddress());
            contextId = result.getContext_id();
            log.info(
                    "Opened StarRocks scanner {}:{} context {} tablets {}",
                    host,
                    port,
                    contextId,
                    split.getTabletIds().size());
        } catch (AresException e) {
            closeQuietly();
            throw e;
        } catch (Exception e) {
            closeQuietly();
            throw new AresException(
                    "Failed to open StarRocks BE scanner " + split.getBeAddress(), e);
        }
    }

    List<AresRow> nextBatch() {
        if (eos) {
            return new ArrayList<>();
        }
        TScanNextBatchParams params = new TScanNextBatchParams();
        params.setContext_id(contextId);
        params.setOffset(offset);
        try {
            TScanBatchResult result = client.get_next(params);
            assertOk(result.getStatus(), "read scanner " + contextId);
            eos = result.isEos();
            List<AresRow> rows = ArrowBatchReader.read(result.getRows(), rowType, allocator);
            offset += rows.size();
            return rows;
        } catch (AresException e) {
            throw e;
        } catch (TException e) {
            throw new AresException("Failed to read StarRocks BE scanner " + contextId, e);
        }
    }

    boolean isEos() {
        return eos;
    }

    @Override
    public void close() {
        if (client != null && contextId != null) {
            TScanCloseParams params = new TScanCloseParams();
            params.setContext_id(contextId);
            try {
                client.close_scanner(params);
            } catch (Exception e) {
                log.warn("Failed to close StarRocks scanner {}", contextId, e);
            }
        }
        closeQuietly();
    }

    private void closeQuietly() {
        if (socket != null) {
            socket.close();
            socket = null;
        }
    }

    private static void assertOk(TStatus status, String action) {
        if (status == null || TStatusCode.OK.equals(status.getStatus_code())) {
            return;
        }
        throw new AresException(
                "StarRocks failed to "
                        + action
                        + ": "
                        + status.getStatus_code()
                        + " "
                        + status.getError_msgs());
    }
}
