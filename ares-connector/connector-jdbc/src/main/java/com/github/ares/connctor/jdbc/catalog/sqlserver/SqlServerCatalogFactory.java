package com.github.ares.connctor.jdbc.catalog.sqlserver;

import com.github.ares.api.table.catalog.Catalog;
import com.github.ares.api.table.factory.CatalogFactory;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connctor.jdbc.catalog.JdbcCatalogOptions;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connctor.jdbc.utils.JdbcUrlUtil;
import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class SqlServerCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.SQLSERVER;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String url = options.get(JdbcCatalogOptions.BASE_URL);
        JdbcUrlUtil.UrlInfo urlInfo = SqlServerURLParser.parse(url);
        return new SqlServerCatalog(
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
