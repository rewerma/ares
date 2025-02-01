package com.github.ares.connctor.jdbc.internal.dialect;

import com.github.ares.connctor.jdbc.internal.converter.AbstractJdbcRowConverter;

public class MysqlJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return DatabaseIdentifier.MYSQL;
    }
}
