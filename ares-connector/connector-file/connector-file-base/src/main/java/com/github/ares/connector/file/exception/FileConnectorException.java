package com.github.ares.connector.file.exception;

import com.github.ares.common.exceptions.AresErrorCode;
import com.github.ares.common.exceptions.AresRuntimeException;

public class FileConnectorException extends AresRuntimeException {
    public FileConnectorException(AresErrorCode aresErrorCode, String errorMessage) {
        super(aresErrorCode, errorMessage);
    }

    public FileConnectorException(
            AresErrorCode aresErrorCode, String errorMessage, Throwable cause) {
        super(aresErrorCode, errorMessage, cause);
    }

    public FileConnectorException(AresErrorCode aresErrorCode, Throwable cause) {
        super(aresErrorCode, cause);
    }
}
