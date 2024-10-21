package com.github.ares.connector.fake.exception;

import com.github.ares.common.exceptions.AresErrorCode;
import com.github.ares.common.exceptions.AresRuntimeException;

public class FakeConnectorException extends AresRuntimeException {
    public FakeConnectorException(AresErrorCode seaTunnelErrorCode, String errorMessage) {
        super(seaTunnelErrorCode, errorMessage);
    }

    public FakeConnectorException(
            AresErrorCode seaTunnelErrorCode, String errorMessage, Throwable cause) {
        super(seaTunnelErrorCode, errorMessage, cause);
    }

    public FakeConnectorException(AresErrorCode seaTunnelErrorCode, Throwable cause) {
        super(seaTunnelErrorCode, cause);
    }
}
