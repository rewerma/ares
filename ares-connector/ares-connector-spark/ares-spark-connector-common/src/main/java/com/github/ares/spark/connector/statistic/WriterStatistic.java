package com.github.ares.spark.connector.statistic;

import java.io.Serializable;

public class WriterStatistic implements Serializable {
    private static final long serialVersionUID = 1L;

    private long readCount;
    private long writeCount;
    private long failedCount;

    public WriterStatistic() {}

    public WriterStatistic(long readCount, long writeCount, long failedCount) {
        this.readCount = readCount;
        this.writeCount = writeCount;
        this.failedCount = failedCount;
    }

    public void incrementRead() {
        readCount++;
    }

    public void incrementWrite() {
        writeCount++;
    }

    public void incrementFailed() {
        failedCount++;
    }

    public long getReadCount() {
        return readCount;
    }

    public long getWriteCount() {
        return writeCount;
    }

    public long getFailedCount() {
        return failedCount;
    }

    public WriterStatistic merge(WriterStatistic other) {
        if (other == null) {
            return this;
        }
        readCount += other.readCount;
        writeCount += other.writeCount;
        failedCount += other.failedCount;
        return this;
    }
}
