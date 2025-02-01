package com.github.ares.common.exceptions;

public class AresException extends RuntimeException {
    private static final long serialVersionUID = -1L;

    public AresException(String errorMessage) {
        super(errorMessage);
    }

    public AresException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

    public AresException(Throwable cause) {
        super(cause);
    }
}
