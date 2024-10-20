package com.github.ares.format.text.exception;


import com.github.ares.common.exceptions.AresException;

public class AresTextFormatException extends AresException {
    public AresTextFormatException(String errorMessage) {
        super( errorMessage);
    }

    public AresTextFormatException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }
}
