package com.github.ares.format.json.exception;

import com.github.ares.common.exceptions.AresErrorCode;
import com.github.ares.common.exceptions.AresRuntimeException;

public class AresJsonFormatException extends AresRuntimeException {
    public AresJsonFormatException(
            AresErrorCode aresErrorCode, String errorMessage) {
        super(aresErrorCode, errorMessage);
    }

    public AresJsonFormatException(
            AresErrorCode aresErrorCode, String errorMessage, Throwable cause) {
        super(aresErrorCode, errorMessage, cause);
    }

    public AresJsonFormatException(AresErrorCode aresErrorCode, Throwable cause) {
        super(aresErrorCode, cause);
    }
}
