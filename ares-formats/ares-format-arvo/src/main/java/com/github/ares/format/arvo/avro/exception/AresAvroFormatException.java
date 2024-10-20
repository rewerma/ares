package com.github.ares.format.arvo.avro.exception;

import com.github.ares.common.exceptions.AresErrorCode;
import com.github.ares.common.exceptions.AresRuntimeException;

public class AresAvroFormatException extends AresRuntimeException {

    public AresAvroFormatException(
            AresErrorCode aresErrorCode, String errorMessage) {
        super(aresErrorCode, errorMessage);
    }
}
