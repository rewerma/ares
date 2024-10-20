package com.github.ares.connctor.jdbc.internal.dialect.db2;

import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectFactory;
import com.google.auto.service.AutoService;

/** Factory for {@link DB2Dialect}. */
@AutoService(JdbcDialectFactory.class)
public class DB2DialectFactory implements JdbcDialectFactory {

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:db2:");
    }

    @Override
    public JdbcDialect create() {
        return new DB2Dialect();
    }
}
