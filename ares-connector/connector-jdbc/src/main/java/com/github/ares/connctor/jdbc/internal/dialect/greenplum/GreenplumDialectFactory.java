package com.github.ares.connctor.jdbc.internal.dialect.greenplum;

import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectFactory;
import com.github.ares.connctor.jdbc.internal.dialect.psql.PostgresDialect;
import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(JdbcDialectFactory.class)
public class GreenplumDialectFactory implements JdbcDialectFactory {

    @Override
    public boolean acceptsURL(@NonNull String url) {
        // Support greenplum native driver: com.pivotal.jdbc.GreenplumDriver
        return url.startsWith("jdbc:pivotal:greenplum:");
    }

    @Override
    public JdbcDialect create() {
        return new PostgresDialect();
    }
}
