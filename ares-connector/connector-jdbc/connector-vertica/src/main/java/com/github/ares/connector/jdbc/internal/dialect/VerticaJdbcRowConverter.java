package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.connector.jdbc.internal.converter.AbstractJdbcRowConverter;

public class VerticaJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return DatabaseIdentifier.VERTICA;
    }
}
