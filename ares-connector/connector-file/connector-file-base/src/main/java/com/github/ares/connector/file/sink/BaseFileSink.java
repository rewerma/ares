package com.github.ares.connector.file.sink;

import com.github.ares.api.common.JobContext;
import com.github.ares.api.sink.AresSink;
import com.github.ares.api.sink.SinkAggregatedCommitter;
import com.github.ares.api.sink.SinkWriter;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.serialization.DefaultSerializer;
import com.github.ares.common.serialization.Serializer;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.exception.FileConnectorErrorCode;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.sink.commit.FileAggregatedCommitInfo;
import com.github.ares.connector.file.sink.commit.FileCommitInfo;
import com.github.ares.connector.file.sink.commit.FileSinkAggregatedCommitter;
import com.github.ares.connector.file.sink.config.FileSinkConfig;
import com.github.ares.connector.file.sink.state.FileSinkState;
import com.github.ares.connector.file.sink.writer.WriteStrategy;
import com.github.ares.connector.file.sink.writer.WriteStrategyFactory;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public abstract class BaseFileSink
        implements AresSink<
        AresRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo> {
    protected AresRowType aresRowType;
    protected Config pluginConfig;
    protected HadoopConf hadoopConf;
    protected FileSinkConfig fileSinkConfig;
    protected JobContext jobContext;
    protected String jobId;

    public BaseFileSink() {
        this.jobId = UUID.randomUUID().toString().replace("-", "");
    }

    @Override
    public void setTypeInfo(AresRowType aresRowType) {
        this.aresRowType = aresRowType;
        this.fileSinkConfig = new FileSinkConfig(pluginConfig, aresRowType);
    }

    @Override
    public SinkWriter<AresRow, FileCommitInfo, FileSinkState> restoreWriter(
            SinkWriter.Context context, List<FileSinkState> states) {
        return new BaseFileSinkWriter(createWriteStrategy(), hadoopConf, context, jobId, states);
    }

    @Override
    public Optional<SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo>>
    createAggregatedCommitter() {
        return Optional.of(new FileSinkAggregatedCommitter(hadoopConf));
    }

    @Override
    public SinkWriter<AresRow, FileCommitInfo, FileSinkState> createWriter(
            SinkWriter.Context context) {
        return new BaseFileSinkWriter(createWriteStrategy(), hadoopConf, context, jobId);
    }

    @Override
    public Optional<Serializer<FileCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<FileAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<FileSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    public void prepare(Config pluginConfig) {
        this.pluginConfig = pluginConfig;
    }

    protected WriteStrategy createWriteStrategy() {
        WriteStrategy writeStrategy =
                WriteStrategyFactory.of(fileSinkConfig.getFileFormat(), fileSinkConfig);
        writeStrategy.setAresRowTypeInfo(aresRowType);
        return writeStrategy;
    }

    @Override
    public void truncateTable(String tableName) {
        try (WriteStrategy writeStrategy =
                     WriteStrategyFactory.of(fileSinkConfig.getFileFormat(), fileSinkConfig)) {
            writeStrategy.init(hadoopConf, jobId, null, 0);
            writeStrategy.truncateFiles();
        } catch (Exception e) {
            throw new AresException(String.format("Truncate table failed: %s, cause %s", tableName, e.getMessage()));
        }
    }
}
