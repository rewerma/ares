package com.github.ares.connector.doris.febe;

import com.github.ares.api.sink.SinkWriter;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DorisFeBeSinkWriter implements SinkWriter<AresRow, Void, Void> {
    private final StreamLoadClient client;
    private final int batchMaxRows;
    private final long batchMaxBytes;
    private final List<AresRow> buffer = new ArrayList<>();
    private long bufferedBytes;

    public DorisFeBeSinkWriter(DorisFeBeConfig config, AresRowType rowType, int subtaskId) {
        this.client = new StreamLoadClient(config, rowType.getFieldNames(), subtaskId);
        this.batchMaxRows = Math.max(config.getBatchMaxRows(), 1);
        this.batchMaxBytes = config.getBatchMaxBytes();
    }

    @Override
    public void write(AresRow element) {
        buffer.add(element);
        bufferedBytes += approximateSize(element);
        if (buffer.size() >= batchMaxRows || bufferedBytes >= batchMaxBytes) {
            flush();
        }
    }

    @Override
    public Optional<Void> prepareCommit() {
        flush();
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
        buffer.clear();
        bufferedBytes = 0;
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        List<AresRow> batch = new ArrayList<>(buffer);
        buffer.clear();
        bufferedBytes = 0;
        client.load(batch);
    }

    private static long approximateSize(AresRow row) {
        long size = 2;
        Object[] fields = row.getFields();
        for (Object field : fields) {
            size += field == null ? 4 : String.valueOf(field).length() + 8;
        }
        return size;
    }
}
