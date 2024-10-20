package com.github.ares.connctor.jdbc.internal.dialect.dm;

import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectFactory;
import com.github.ares.connctor.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import com.google.auto.service.AutoService;

/** Factory for {@link DmdbDialect}. */
@AutoService(JdbcDialectFactory.class)
public class DmdbDialectFactory implements JdbcDialectFactory {

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:dm:");
    }

    @Override
    public JdbcDialect create() {
        return create(null, FieldIdeEnum.ORIGINAL.getValue());
    }

    @Override
    public JdbcDialect create(String compatibleMode, String fieldIde) {
        return new DmdbDialect(fieldIde);
    }
}
