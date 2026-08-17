package com.github.ares.spark.connector.sink.write;

import com.github.ares.spark.connector.statistic.WriterStatistic;
import javax.annotation.Nullable;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

public class AresSparkWriterCommitMessage<T> implements WriterCommitMessage {

    private @Nullable T message;
    private WriterStatistic statistic;

    AresSparkWriterCommitMessage(T message, WriterStatistic statistic) {
        this.message = message;
        this.statistic = statistic;
    }

    public T getMessage() {
        return message;
    }

    public void setMessage(T message) {
        this.message = message;
    }

    public WriterStatistic getStatistic() {
        return statistic;
    }
}
