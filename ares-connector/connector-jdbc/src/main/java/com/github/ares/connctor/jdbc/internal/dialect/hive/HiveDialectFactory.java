package com.github.ares.connctor.jdbc.internal.dialect.hive;

import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectFactory;
import com.google.auto.service.AutoService;

@AutoService(JdbcDialectFactory.class)
public class HiveDialectFactory implements JdbcDialectFactory {

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:hive2:");
    }

    @Override
    public JdbcDialect create() {
        return new HiveDialect();
    }
}
