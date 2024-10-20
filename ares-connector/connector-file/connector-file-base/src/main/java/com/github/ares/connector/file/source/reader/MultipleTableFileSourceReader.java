package com.github.ares.connector.file.source.reader;

import com.github.ares.api.source.Collector;
import com.github.ares.api.source.SourceReader;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.connector.file.config.BaseMultipleTableFileSourceConfig;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.source.split.FileSourceSplit;
import com.github.ares.connector.file.exception.FileConnectorErrorCode;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

import com.github.ares.connector.file.config.BaseFileSourceConfig;

@Slf4j
public class MultipleTableFileSourceReader implements SourceReader<AresRow, FileSourceSplit> {

    private final Context context;
    private volatile boolean noMoreSplit;

    private final Deque<FileSourceSplit> sourceSplits = new ConcurrentLinkedDeque<>();

    private final Map<String, ReadStrategy> readStrategyMap;

    public MultipleTableFileSourceReader(
            SourceReader.Context context, BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig) {
        this.context = context;
        this.readStrategyMap =
                multipleTableFileSourceConfig.getFileSourceConfigs().stream()
                        .collect(
                                Collectors.toMap(
                                        fileSourceConfig ->
                                                fileSourceConfig
                                                        .getCatalogTable()
                                                        .getTableId()
                                                        .toTablePath()
                                                        .toString(),
                                        BaseFileSourceConfig::getReadStrategy));
    }

    @Override
    public void pollNext(Collector<AresRow> output) {
        synchronized (output.getCheckpointLock()) {
            FileSourceSplit split = sourceSplits.poll();
            if (null != split) {
                ReadStrategy readStrategy = readStrategyMap.get(split.getTableId());
                if (readStrategy == null) {
                    throw new FileConnectorException(
                            FileConnectorErrorCode.FILE_READ_STRATEGY_NOT_SUPPORT,
                            "Cannot found the read strategy for this table: ["
                                    + split.getTableId()
                                    + "]");
                }
                try {
                    readStrategy.read(split.getFilePath(), split.getTableId(), output);
                } catch (Exception e) {
                    String errorMsg =
                            String.format("Read data from this file [%s] failed", split.splitId());
                    throw new FileConnectorException(FileConnectorErrorCode.FILE_READ_FAILED, errorMsg, e);
                }
            } else if (noMoreSplit && sourceSplits.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                log.info(
                        "There is no more element for the bounded MultipleTableLocalFileSourceReader");
                context.signalNoMoreElement();
            }
        }
    }

    @Override
    public List<FileSourceSplit> snapshotState(long checkpointId) {
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
//    public void notifyCheckpointComplete(long checkpointId) {
//        // do nothing
//    }

    @Override
    public void open() throws Exception {
        // do nothing
        log.info("Opened the MultipleTableLocalFileSourceReader");
    }

    @Override
    public void close() throws IOException {
        // do nothing
        log.info("Closed the MultipleTableLocalFileSourceReader");
        for (ReadStrategy strategy : readStrategyMap.values()) {
            strategy.close();
        }
    }
}
