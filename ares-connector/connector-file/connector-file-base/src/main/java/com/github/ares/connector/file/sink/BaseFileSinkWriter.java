package com.github.ares.connector.file.sink;

import com.github.ares.api.sink.SinkWriter;
import com.github.ares.api.sink.SupportMultiTableSinkWriter;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.exceptions.AresRuntimeException;
import com.github.ares.common.exceptions.CommonError;
import com.github.ares.common.exceptions.CommonErrorCode;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.hadoop.HadoopFileSystemProxy;
import com.github.ares.connector.file.sink.commit.FileAggregatedCommitInfo;
import com.github.ares.connector.file.sink.commit.FileCommitInfo;
import com.github.ares.connector.file.sink.commit.FileSinkAggregatedCommitter;
import com.github.ares.connector.file.sink.state.FileSinkState;
import com.github.ares.connector.file.sink.writer.AbstractWriteStrategy;
import com.github.ares.connector.file.sink.writer.WriteStrategy;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class BaseFileSinkWriter
        implements SinkWriter<AresRow, FileCommitInfo, FileSinkState>,
        SupportMultiTableSinkWriter<WriteStrategy> {

    protected final WriteStrategy writeStrategy;

    public BaseFileSinkWriter(
            WriteStrategy writeStrategy,
            HadoopConf hadoopConf,
            SinkWriter.Context context,
            String jobId,
            List<FileSinkState> fileSinkStates) {
        this.writeStrategy = writeStrategy;
        int subTaskIndex = context.getIndexOfSubtask();
        String uuidPrefix;
        if (!fileSinkStates.isEmpty()) {
            uuidPrefix = fileSinkStates.get(0).getUuidPrefix();
        } else {
            uuidPrefix = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 10);
        }
        writeStrategy.init(hadoopConf, jobId, uuidPrefix, subTaskIndex);
        final HadoopFileSystemProxy hadoopFileSystemProxy =
                writeStrategy.getHadoopFileSystemProxy();
        if (!fileSinkStates.isEmpty()) {
            try {
                List<String> transactions =
                        findTransactionList(jobId, uuidPrefix, hadoopFileSystemProxy);
                FileSinkAggregatedCommitter fileSinkAggregatedCommitter =
                        new FileSinkAggregatedCommitter(hadoopConf);
                fileSinkAggregatedCommitter.init();
                LinkedHashMap<String, FileSinkState> fileStatesMap = new LinkedHashMap<>();
                fileSinkStates.forEach(
                        fileSinkState ->
                                fileStatesMap.put(fileSinkState.getTransactionId(), fileSinkState));
                for (String transaction : transactions) {
                    if (fileStatesMap.containsKey(transaction)) {
                        // need commit
                        FileSinkState fileSinkState = fileStatesMap.get(transaction);
                        FileAggregatedCommitInfo fileCommitInfo =
                                fileSinkAggregatedCommitter.combine(
                                        Collections.singletonList(
                                                new FileCommitInfo(
                                                        fileSinkState.getNeedMoveFiles(),
                                                        fileSinkState.getPartitionDirAndValuesMap(),
                                                        fileSinkState.getTransactionDir())));
                        fileSinkAggregatedCommitter.commit(
                                Collections.singletonList(fileCommitInfo));
                    } else {
                        // need abort
                        writeStrategy.abortPrepare(transaction);
                    }
                }
            } catch (IOException e) {
                String errorMsg =
                        String.format("Try to process these fileStates %s failed", fileSinkStates);
                throw new FileConnectorException(
                        CommonErrorCode.WRITER_OPERATION_FAILED, errorMsg, e);
            }
            writeStrategy.beginTransaction(fileSinkStates.get(0).getCheckpointId() + 1);
        } else {
            writeStrategy.beginTransaction(1L);
        }
    }

    private List<String> findTransactionList(
            String jobId, String uuidPrefix, HadoopFileSystemProxy hadoopFileSystemProxy)
            throws IOException {
        return hadoopFileSystemProxy
                .getAllSubFiles(
                        AbstractWriteStrategy.getTransactionDirPrefix(
                                writeStrategy.getFileSinkConfig().getTmpPath(), jobId, uuidPrefix))
                .stream()
                .map(Path::getName)
                .collect(Collectors.toList());
    }

    public BaseFileSinkWriter(
            WriteStrategy writeStrategy,
            HadoopConf hadoopConf,
            SinkWriter.Context context,
            String jobId) {
        this(writeStrategy, hadoopConf, context, jobId, Collections.emptyList());
        writeStrategy.beginTransaction(1L);
    }

    @Override
    public void write(AresRow element) throws IOException {
        try {
            writeStrategy.write(element);
        } catch (AresRuntimeException e) {
            throw CommonError.writeAresRowFailed("FileConnector", element.toString(), e);
        }
    }

    @Override
    public Optional<FileCommitInfo> prepareCommit() throws IOException {
        return writeStrategy.prepareCommit();
    }

    @Override
    public void abortPrepare() {
        writeStrategy.abortPrepare();
    }

    @Override
    public List<FileSinkState> snapshotState(long checkpointId) throws IOException {
        return writeStrategy.snapshotState(checkpointId);
    }

    @Override
    public void close() throws IOException {
        if (writeStrategy != null) {
            writeStrategy.close();
        }
    }

    public WriteStrategy getWriteStrategy() {
        return writeStrategy;
    }
}
