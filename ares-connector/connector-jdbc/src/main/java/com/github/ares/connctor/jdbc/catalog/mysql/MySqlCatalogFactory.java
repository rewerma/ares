package com.github.ares.connctor.jdbc.catalog.mysql;

import com.github.ares.api.table.catalog.Catalog;
import com.github.ares.api.table.factory.CatalogFactory;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.com.google.common.base.Preconditions;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connctor.jdbc.catalog.JdbcCatalogOptions;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connctor.jdbc.utils.JdbcUrlUtil;
import com.google.auto.service.AutoService;
import org.apache.commons.lang3.StringUtils;

@AutoService(Factory.class)
public class MySqlCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.MYSQL;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String urlWithDatabase = options.get(JdbcCatalogOptions.BASE_URL);
        Preconditions.checkArgument(
                StringUtils.isNoneBlank(urlWithDatabase),
                "Miss config <base-url>! Please check your config.");
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(urlWithDatabase);
        return new MySqlCatalog(
                catalogName,
                options.get(JdbcCatalogOptions.USERNAME),
                options.get(JdbcCatalogOptions.PASSWORD),
                urlInfo);
    }

    @Override
    public OptionRule optionRule() {
        return JdbcCatalogOptions.BASE_RULE.build();
    }
}
