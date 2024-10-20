package com.github.ares.spark.connector.source.partition.micro;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.table.type.AresRow;
import org.apache.spark.sql.connector.read.InputPartition;

public class AresMicroBatchInputPartition implements InputPartition {
    protected final AresSource<AresRow, ?, ?> source;
    protected final Integer parallelism;
    protected final Integer subtaskId;
    protected final Integer checkpointId;
    protected final Integer checkpointInterval;
    protected final String checkpointPath;
    protected final String hdfsRoot;
    protected final String hdfsUser;

    public AresMicroBatchInputPartition(
            AresSource<AresRow, ?, ?> source,
            Integer parallelism,
            Integer subtaskId,
            Integer checkpointId,
            Integer checkpointInterval,
            String checkpointPath,
            String hdfsRoot,
            String hdfsUser) {
        this.source = source;
        this.parallelism = parallelism;
        this.subtaskId = subtaskId;
        this.checkpointId = checkpointId;
        this.checkpointInterval = checkpointInterval;
        this.checkpointPath = checkpointPath;
        this.hdfsRoot = hdfsRoot;
        this.hdfsUser = hdfsUser;
    }

    public AresSource<AresRow, ?, ?> getSource() {
        return source;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public Integer getSubtaskId() {
        return subtaskId;
    }

    public Integer getCheckpointId() {
        return checkpointId;
    }

    public Integer getCheckpointInterval() {
        return checkpointInterval;
    }

    public String getCheckpointPath() {
        return checkpointPath;
    }

    public String getHdfsRoot() {
        return hdfsRoot;
    }

    public String getHdfsUser() {
        return hdfsUser;
    }
}
