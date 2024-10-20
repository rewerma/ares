package com.github.ares.connctor.jdbc.internal.dialect.db2;

import com.github.ares.connctor.jdbc.internal.converter.AbstractJdbcRowConverter;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;

public class DB2JdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String converterName() {
        return DatabaseIdentifier.DB_2;
    }
}
