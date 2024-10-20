package com.github.ares.connector.file.sink;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.sink.SinkAggregatedCommitter;
import com.github.ares.api.sink.SinkWriter;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.serialization.DefaultSerializer;
import com.github.ares.common.serialization.Serializer;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.hadoop.HadoopFileSystemProxy;
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

public abstract class BaseMultipleTableFileSink
        implements AresSink<
        AresRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo> {

    private final HadoopConf hadoopConf;
    private final CatalogTable catalogTable;
    private final FileSinkConfig fileSinkConfig;
    private String jobId;

    public abstract String getPluginName();

    public BaseMultipleTableFileSink(
            HadoopConf hadoopConf, ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        this.hadoopConf = hadoopConf;
        this.fileSinkConfig =
                new FileSinkConfig(readonlyConfig.toConfig(), catalogTable.getAresRowType());
        this.catalogTable = catalogTable;
        this.jobId = UUID.randomUUID().toString().replace("-", "");
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

    protected WriteStrategy createWriteStrategy() {
        WriteStrategy writeStrategy =
                WriteStrategyFactory.of(fileSinkConfig.getFileFormat(), fileSinkConfig);
        writeStrategy.setAresRowTypeInfo(catalogTable.getAresRowType());
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
