package com.github.ares.connector.file.source;

import com.github.ares.api.source.Collector;
import com.github.ares.api.source.SourceReader;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.exceptions.CommonError;
import com.github.ares.connector.file.source.reader.ReadStrategy;
import com.github.ares.connector.file.source.split.FileSourceSplit;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
public class BaseFileSourceReader implements SourceReader<AresRow, FileSourceSplit> {
    private final ReadStrategy readStrategy;
    private final SourceReader.Context context;
    private final Deque<FileSourceSplit> sourceSplits = new ConcurrentLinkedDeque<>();
    private volatile boolean noMoreSplit;

    public BaseFileSourceReader(ReadStrategy readStrategy, SourceReader.Context context) {
        this.readStrategy = readStrategy;
        this.context = context;
    }

    @Override
    public void open() throws Exception {}

    @Override
    public void close() throws IOException {
        readStrategy.close();
    }

    @Override
    public void pollNext(Collector<AresRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            FileSourceSplit split = sourceSplits.poll();
            if (null != split) {
                try {
                    // todo: If there is only one table , the tableId is not needed, but it's better
                    // to set this
                    readStrategy.read(split.splitId(), "", output);
                } catch (Exception e) {
                    throw CommonError.fileOperationFailed("Ares", "read", split.splitId(), e);
                }
            } else if (noMoreSplit && sourceSplits.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded File source");
                context.signalNoMoreElement();
            } else {
                Thread.sleep(1000L);
            }
        }
    }

    @Override
    public List<FileSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(sourceSplits);
    }

    @Override
    public void addSplits(List<FileSourceSplit> splits) {
        sourceSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

//    @Override
//    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
