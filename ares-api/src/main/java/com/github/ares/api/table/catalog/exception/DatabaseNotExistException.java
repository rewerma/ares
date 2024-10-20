package com.github.ares.api.table.catalog.exception;

import com.github.ares.common.exceptions.AresException;

/** Exception for trying to operate on a database that doesn't exist. */
public class DatabaseNotExistException extends AresException {
    private static final String MSG = "Database %s does not exist in Catalog %s.";

    public DatabaseNotExistException(String catalogName, String databaseName, Throwable cause) {
        super(
                String.format(MSG, databaseName, catalogName),
                cause);
    }

    public DatabaseNotExistException(String catalogName, String databaseName) {
        this(catalogName, databaseName, null);
    }
}
