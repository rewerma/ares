package com.github.ares.spark.connector.source.partition.micro;

import com.github.ares.common.utils.JsonUtils;
import org.apache.spark.sql.connector.read.streaming.Offset;

import java.io.Serializable;

public class AresOffset extends Offset implements Serializable {

    private final long checkpointId;

    public AresOffset(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    @Override
    public String json() {
        return JsonUtils.toJsonString(this);
    }

    public AresOffset inc() {
        return new AresOffset(this.checkpointId + 1);
    }

    public static Offset of(long checkpointId) {
        return new AresOffset(checkpointId);
    }

    public long getCheckpointId() {
        return checkpointId;
    }
}
