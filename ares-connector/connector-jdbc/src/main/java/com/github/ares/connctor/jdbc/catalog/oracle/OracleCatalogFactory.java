package com.github.ares.connctor.jdbc.catalog.oracle;

import com.github.ares.api.table.catalog.Catalog;
import com.github.ares.api.table.factory.CatalogFactory;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connctor.jdbc.catalog.JdbcCatalogOptions;
import com.github.ares.connctor.jdbc.exception.JdbcConnectorException;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connctor.jdbc.utils.JdbcUrlUtil;
import com.google.auto.service.AutoService;

import java.util.Optional;

@AutoService(Factory.class)
public class OracleCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.ORACLE;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String urlWithDatabase = options.get(JdbcCatalogOptions.BASE_URL);
        JdbcUrlUtil.UrlInfo urlInfo = OracleURLParser.parse(urlWithDatabase);
        Optional<String> defaultDatabase = urlInfo.getDefaultDatabase();
        if (!defaultDatabase.isPresent()) {
            throw new JdbcConnectorException("Database undefined " + JdbcCatalogOptions.BASE_URL);
        }
        return new OracleCatalog(
                catalogName,
                options.get(JdbcCatalogOptions.USERNAME),
                options.get(JdbcCatalogOptions.PASSWORD),
                urlInfo,
                options.get(JdbcCatalogOptions.SCHEMA));
    }

    @Override
    public OptionRule optionRule() {
        return JdbcCatalogOptions.BASE_RULE.build();
    }
}
