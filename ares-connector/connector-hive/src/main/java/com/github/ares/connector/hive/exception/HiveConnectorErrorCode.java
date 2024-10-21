package com.github.ares.connector.hive.exception;


import com.github.ares.common.exceptions.AresErrorCode;

public enum HiveConnectorErrorCode implements AresErrorCode {
    GET_HDFS_NAMENODE_HOST_FAILED("HIVE-01", "Get name node host from table location failed"),
    INITIALIZE_HIVE_METASTORE_CLIENT_FAILED("HIVE-02", "Initialize hive metastore client failed"),
    GET_HIVE_TABLE_INFORMATION_FAILED(
            "HIVE-03", "Get hive table information from hive metastore service failed");

    private final String code;
    private final String description;

    HiveConnectorErrorCode(String code, String description) {
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
