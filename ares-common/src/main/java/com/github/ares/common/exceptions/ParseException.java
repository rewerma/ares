package com.github.ares.common.exceptions;

public class ParseException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ParseException(String errorMessage) {
        super(errorMessage);
    }

    public ParseException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

    public ParseException(Throwable cause) {
        super(cause);
    }
}
