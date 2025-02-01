package com.github.ares.connctor.jdbc.exception;

import com.github.ares.common.exceptions.AresException;

public class JdbcConnectorException extends AresException {
    public JdbcConnectorException(String errorMessage) {
        super(errorMessage);
    }

    public JdbcConnectorException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }
}
