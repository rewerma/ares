package com.github.ares.connctor.jdbc.catalog.oceanbase;

import com.github.ares.api.table.catalog.Catalog;
import com.github.ares.api.table.factory.CatalogFactory;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.com.google.common.base.Preconditions;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connctor.jdbc.catalog.JdbcCatalogOptions;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connctor.jdbc.utils.JdbcUrlUtil;
import com.google.auto.service.AutoService;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

@AutoService(Factory.class)
public class OceanBaseCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.OCENABASE;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String urlWithDatabase = options.get(JdbcCatalogOptions.BASE_URL);
        Preconditions.checkArgument(
                StringUtils.isNoneBlank(urlWithDatabase),
                "Miss config <base-url>! Please check your config.");
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(urlWithDatabase);
        Optional<String> defaultDatabase = urlInfo.getDefaultDatabase();
        if (!defaultDatabase.isPresent()) {
            throw new AresException("Default database is not set in url: " + JdbcCatalogOptions.BASE_URL);
        }

        String compatibleMode = options.get(JdbcCatalogOptions.COMPATIBLE_MODE);
        Preconditions.checkArgument(
                StringUtils.isNoneBlank(compatibleMode),
                "Miss config <compatibleMode>! Please check your config.");

        if ("oracle".equalsIgnoreCase(compatibleMode.trim())) {
            return new OceanBaseOracleCatalog(
                    catalogName,
                    options.get(JdbcCatalogOptions.USERNAME),
                    options.get(JdbcCatalogOptions.PASSWORD),
                    urlInfo,
                    options.get(JdbcCatalogOptions.SCHEMA));
        }
        return new OceanBaseMySqlCatalog(
                catalogName,
                options.get(JdbcCatalogOptions.USERNAME),
                options.get(JdbcCatalogOptions.PASSWORD),
                urlInfo);
    }

    @Override
    public OptionRule optionRule() {
        return JdbcCatalogOptions.BASE_RULE.required(JdbcCatalogOptions.COMPATIBLE_MODE).build();
    }
}
