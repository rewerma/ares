package com.github.ares.connector.fake.exception;

import com.github.ares.common.exceptions.AresErrorCode;
import com.github.ares.common.exceptions.AresRuntimeException;

public class FakeConnectorException extends AresRuntimeException {
    public FakeConnectorException(AresErrorCode aresErrorCode, String errorMessage) {
        super(aresErrorCode, errorMessage);
    }

    public FakeConnectorException(
            AresErrorCode aresErrorCode, String errorMessage, Throwable cause) {
        super(aresErrorCode, errorMessage, cause);
    }

    public FakeConnectorException(AresErrorCode aresErrorCode, Throwable cause) {
        super(aresErrorCode, cause);
    }
}
