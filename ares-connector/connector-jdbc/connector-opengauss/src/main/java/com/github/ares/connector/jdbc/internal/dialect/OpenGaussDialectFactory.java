package com.github.ares.connector.jdbc.internal.dialect;

import com.google.auto.service.AutoService;
import javax.annotation.Nonnull;

/** Factory for {@link OpenGaussDialect}. */
@AutoService(JdbcDialectFactory.class)
public class OpenGaussDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectIdentifier() {
        return DatabaseIdentifier.OPENGAUSS;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:opengauss:");
    }

    @Override
    public JdbcDialect create() {
        return new OpenGaussDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new OpenGaussDialect(fieldIde);
    }
}
