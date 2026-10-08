package com.github.ares.connector.doris.febe;

import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.AresException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.doris.sdk.thrift.TDorisExternalService;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanCloseParams;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;
import org.apache.doris.sdk.thrift.TScanOpenParams;
import org.apache.doris.sdk.thrift.TScanOpenResult;
import org.apache.doris.sdk.thrift.TStatus;
import org.apache.doris.sdk.thrift.TStatusCode;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Opens one tablet scanner on a BE thrift port and reads Arrow batches. */
final class BeScanClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BeScanClient.class);
    private static final String DEFAULT_CLUSTER = "default_cluster";

    private final DorisSplit split;
    private final DorisFeBeConfig config;
    private final AresRowType rowType;
    private final BufferAllocator allocator;
    private TSocket socket;
    private TDorisExternalService.Client client;
    private String contextId;
    private long offset;
    private boolean eos;

    BeScanClient(
            DorisSplit split,
            DorisFeBeConfig config,
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
                    "Doris BE address must be host:port, got " + split.getBeAddress());
        }
        String host = hostPort[0].trim();
        int port = Integer.parseInt(hostPort[1].trim());
        try {
            socket =
                    new TSocket(new TConfiguration(), host, port, config.getScanConnectTimeoutMs());
            socket.open();
            client = new TDorisExternalService.Client(new TBinaryProtocol(socket));
            TScanOpenParams params = new TScanOpenParams();
            params.setTabletIds(new ArrayList<>(split.getTabletIds()));
            params.setOpaquedQueryPlan(split.getOpaqueQueryPlan());
            params.setCluster(DEFAULT_CLUSTER);
            params.setDatabase(split.getDatabase());
            params.setTable(split.getTable());
            params.setUser(config.getUsername());
            params.setPasswd(config.getPassword());
            params.setBatchSize(config.getScanBatchRows());
            params.setKeepAliveMin((short) Math.min(Short.MAX_VALUE, config.getScanKeepAliveMin()));
            params.setQueryTimeout(config.getScanQueryTimeoutSec());
            params.setMemLimit(config.getScanMemLimit());
            TScanOpenResult result = client.openScanner(params);
            assertOk(result.getStatus(), "open scanner on " + split.getBeAddress());
            contextId = result.getContextId();
            log.info(
                    "Opened Doris scanner {}:{} context {} tablets {}",
                    host,
                    port,
                    contextId,
                    split.getTabletIds().size());
        } catch (AresException e) {
            closeQuietly();
            throw e;
        } catch (Exception e) {
            closeQuietly();
            throw new AresException("Failed to open Doris BE scanner " + split.getBeAddress(), e);
        }
    }

    List<AresRow> nextBatch() {
        if (eos) {
            return new ArrayList<>();
        }
        TScanNextBatchParams params = new TScanNextBatchParams();
        params.setContextId(contextId);
        params.setOffset(offset);
        try {
            TScanBatchResult result = client.getNext(params);
            assertOk(result.getStatus(), "read scanner " + contextId);
            eos = result.isEos();
            List<AresRow> rows = ArrowBatchReader.read(result.getRows(), rowType, allocator);
            offset += rows.size();
            return rows;
        } catch (AresException e) {
            throw e;
        } catch (TException e) {
            throw new AresException("Failed to read Doris BE scanner " + contextId, e);
        }
    }

    boolean isEos() {
        return eos;
    }

    @Override
    public void close() {
        if (client != null && contextId != null) {
            TScanCloseParams params = new TScanCloseParams();
            params.setContextId(contextId);
            try {
                client.closeScanner(params);
            } catch (Exception e) {
                log.warn("Failed to close Doris scanner {}", contextId, e);
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
        if (status == null || TStatusCode.OK.equals(status.getStatusCode())) {
            return;
        }
        throw new AresException(
                "Doris failed to "
                        + action
                        + ": "
                        + status.getStatusCode()
                        + " "
                        + status.getErrorMsgs());
    }
}
