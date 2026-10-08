package com.github.ares.connector.doris.febe;

import com.github.ares.api.source.Collector;
import com.github.ares.api.source.SourceReader;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.AresException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class DorisFeBeSourceReader implements SourceReader<AresRow, DorisSplit> {
    private final Context context;
    private final DorisFeBeConfig config;
    private final AresRowType rowType;
    private final Deque<DorisSplit> pending = new ArrayDeque<>();
    private final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    private BeScanClient current;
    private Iterator<AresRow> rows = Collections.emptyIterator();
    private volatile boolean noMoreSplit;

    public DorisFeBeSourceReader(Context context, DorisFeBeConfig config, AresRowType rowType) {
        this.context = context;
        this.config = config;
        this.rowType = rowType;
    }

    @Override
    public void open() {}

    @Override
    public void close() throws IOException {
        closeCurrent();
        allocator.close();
    }

    @Override
    public void pollNext(Collector<AresRow> output) {
        synchronized (output.getCheckpointLock()) {
            if (!rows.hasNext()) {
                rows = nextRows().iterator();
            }
            if (rows.hasNext()) {
                output.collect(rows.next());
                return;
            }
        }
        if (noMoreSplit && pending.isEmpty() && current == null && !rows.hasNext()) {
            context.signalNoMoreElement();
        }
    }

    private List<AresRow> nextRows() {
        while (true) {
            if (current != null && !current.isEos()) {
                List<AresRow> batch = current.nextBatch();
                if (!batch.isEmpty()) {
                    return batch;
                }
            }
            if (current != null && current.isEos()) {
                closeCurrent();
            }
            if (current != null) {
                throw new AresException(
                        "Doris BE scanner returned an empty batch before end of scan");
            }
            DorisSplit split = pending.poll();
            if (split == null) {
                return Collections.emptyList();
            }
            current = new BeScanClient(split, config, rowType, allocator);
            current.open();
        }
    }

    @Override
    public List<DorisSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(pending);
    }

    @Override
    public void addSplits(List<DorisSplit> splits) {
        pending.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    private void closeCurrent() {
        if (current != null) {
            current.close();
            current = null;
        }
    }
}
