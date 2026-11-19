package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.connector.jdbc.internal.converter.AbstractJdbcRowConverter;

public class Db2JdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return DatabaseIdentifier.DB_2;
    }
}
