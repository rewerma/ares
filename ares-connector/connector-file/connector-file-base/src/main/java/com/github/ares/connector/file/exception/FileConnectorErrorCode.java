package com.github.ares.connector.file.exception;

import com.github.ares.common.exceptions.AresErrorCode;

public enum FileConnectorErrorCode implements AresErrorCode {
    FILE_TYPE_INVALID("FILE-01", "File type is invalid"),
    DATA_DESERIALIZE_FAILED("FILE-02", "Data deserialization failed"),
    FILE_LIST_GET_FAILED("FILE-03", "Get file list failed"),
    FILE_LIST_EMPTY("FILE-04", "File list is empty"),
    AGGREGATE_COMMIT_ERROR("FILE-05", "Aggregate committer error"),
    FILE_READ_STRATEGY_NOT_SUPPORT("FILE-06", "File strategy not support"),
    FORMAT_NOT_SUPPORT("FILE-07", "Format not support"),
    FILE_READ_FAILED("FILE-08", "File read failed"),
    ;

    private final String code;
    private final String description;

    FileConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
