package com.github.ares.connctor.jdbc.internal.dialect.base;

import com.github.ares.connctor.jdbc.internal.converter.AbstractJdbcRowConverter;
import com.github.ares.connctor.jdbc.internal.converter.JdbcRowConverter;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectTypeMapper;


public class CommonDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return DatabaseIdentifier.COMMON;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return  new AbstractJdbcRowConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public String converterName() {
                return DatabaseIdentifier.COMMON;
            }
        };
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new CommonTypeMapper();
    }
}
