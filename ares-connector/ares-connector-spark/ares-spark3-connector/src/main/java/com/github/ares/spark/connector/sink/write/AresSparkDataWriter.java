package com.github.ares.spark.connector.sink.write;

import com.github.ares.api.sink.SinkCommitter;
import com.github.ares.api.sink.SinkWriter;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.connector.serialization.RowConverter;
import com.github.ares.spark.connector.serialization.InternalRowConverter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

public class AresSparkDataWriter<CommitInfoT, StateT> implements DataWriter<InternalRow> {

    private final SinkWriter<AresRow, CommitInfoT, StateT> sinkWriter;

    @Nullable private final SinkCommitter<CommitInfoT> sinkCommitter;
    private final RowConverter<InternalRow> rowConverter;
    private CommitInfoT latestCommitInfoT;
    private long epochId;

    public AresSparkDataWriter(
            SinkWriter<AresRow, CommitInfoT, StateT> sinkWriter,
            @Nullable SinkCommitter<CommitInfoT> sinkCommitter,
            AresDataType<?> dataType,
            long epochId) {
        this.sinkWriter = sinkWriter;
        this.sinkCommitter = sinkCommitter;
        this.rowConverter = new InternalRowConverter(dataType);
        this.epochId = epochId == 0 ? 1 : epochId;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        sinkWriter.write(rowConverter.reconvert(record));
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        Optional<CommitInfoT> commitInfoTOptional = sinkWriter.prepareCommit();
        commitInfoTOptional.ifPresent(commitInfoT -> latestCommitInfoT = commitInfoT);
        sinkWriter.snapshotState(epochId++);
        if (sinkCommitter != null) {
            if (latestCommitInfoT == null) {
                sinkCommitter.commit(Collections.emptyList());
            } else {
                sinkCommitter.commit(Collections.singletonList(latestCommitInfoT));
            }
        }
        AresSparkWriterCommitMessage<CommitInfoT> aresSparkWriterCommitMessage =
                new AresSparkWriterCommitMessage<>(latestCommitInfoT);
        cleanCommitInfo();
        sinkWriter.close();
        return aresSparkWriterCommitMessage;
    }

    @Override
    public void abort() throws IOException {
        sinkWriter.abortPrepare();
        if (sinkCommitter != null) {
            if (latestCommitInfoT == null) {
                sinkCommitter.abort(Collections.emptyList());
            } else {
                sinkCommitter.abort(Collections.singletonList(latestCommitInfoT));
            }
        }
        cleanCommitInfo();
    }

    private void cleanCommitInfo() {
        latestCommitInfoT = null;
    }

    @Override
    public void close() throws IOException {}
}
