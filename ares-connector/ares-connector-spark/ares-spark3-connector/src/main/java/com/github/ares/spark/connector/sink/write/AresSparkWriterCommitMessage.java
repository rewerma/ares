package com.github.ares.spark.connector.sink.write;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

import javax.annotation.Nullable;

public class AresSparkWriterCommitMessage<T> implements WriterCommitMessage {

    private @Nullable T message;

    AresSparkWriterCommitMessage(T message) {
        this.message = message;
    }

    public T getMessage() {
        return message;
    }

    public void setMessage(T message) {
        this.message = message;
    }
}
