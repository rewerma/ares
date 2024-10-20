package com.github.ares.sql.expression.exception;

public class ExpressionException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ExpressionException(String errorMessage) {
        super(errorMessage);
    }

    public ExpressionException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }
}
