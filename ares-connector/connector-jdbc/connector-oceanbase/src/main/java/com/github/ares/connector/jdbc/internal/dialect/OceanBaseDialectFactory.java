package com.github.ares.connector.jdbc.internal.dialect;

import com.google.auto.service.AutoService;
import java.util.Locale;
import javax.annotation.Nonnull;

/** Factory for OceanBase MySQL and Oracle compatible dialects. */
@AutoService(JdbcDialectFactory.class)
public class OceanBaseDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectIdentifier() {
        return DatabaseIdentifier.OCEANBASE;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:oceanbase:");
    }

    @Override
    public JdbcDialect create() {
        return new OceanBaseMysqlDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        if (compatibleMode != null && compatibleMode.toLowerCase(Locale.ROOT).contains("oracle")) {
            return new OceanBaseOracleDialect(fieldIde);
        }
        return new OceanBaseMysqlDialect(fieldIde);
    }
}
