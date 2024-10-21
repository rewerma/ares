package com.github.ares.connector.hive.exception;

import com.github.ares.common.exceptions.AresErrorCode;
import com.github.ares.common.exceptions.AresRuntimeException;

public class HiveConnectorException extends AresRuntimeException {
    public HiveConnectorException(AresErrorCode aresErrorCode, String errorMessage) {
        super(aresErrorCode, errorMessage);
    }

    public HiveConnectorException(
            AresErrorCode aresErrorCode, String errorMessage, Throwable cause) {
        super(aresErrorCode, errorMessage, cause);
    }

    public HiveConnectorException(AresErrorCode aresErrorCode, Throwable cause) {
        super(aresErrorCode, cause);
    }
}
