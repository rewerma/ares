package com.github.ares.connector.jdbc.source;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.SourceSplit;
import com.github.ares.api.source.TableSource;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSourceFactory;
import com.github.ares.api.table.factory.TableSourceFactoryContext;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connector.jdbc.config.JdbcOptions;
import com.github.ares.connector.jdbc.config.JdbcSourceOptions;
import com.github.ares.connector.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connector.starrocks.config.StarRocksOptions;
import com.github.ares.connector.starrocks.febe.StarRocksFeBeConfig;
import com.github.ares.connector.starrocks.febe.StarRocksFeBeSource;
import com.github.ares.connector.starrocks.util.StarRocksJdbcDefaults;
import com.google.auto.service.AutoService;
import java.io.Serializable;

@AutoService(Factory.class)
public class StarRocksSourceFactory extends JdbcSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.STARROCKS;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        StarRocksFeBeConfig.checkMode(context.getOptions());
        if (StarRocksFeBeConfig.isFeBe(context.getOptions())) {
            return () ->
                    (AresSource<T, SplitT, StateT>) new StarRocksFeBeSource(context.getOptions());
        }
        if (!context.getOptions().getOptional(JdbcOptions.URL).isPresent()) {
            throw new AresException(
                    "StarRocks jdbc mode requires url, for example jdbc:mysql://fe_host:9030/database");
        }
        ReadonlyConfig options = StarRocksJdbcDefaults.apply(context.getOptions());
        return super.createSource(new TableSourceFactoryContext(options, context.getClassLoader()));
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(
                        StarRocksOptions.MODE,
                        StarRocksOptions.FE_NODES,
                        JdbcOptions.URL,
                        JdbcOptions.DRIVER,
                        JdbcOptions.USER,
                        JdbcOptions.PASSWORD,
                        JdbcOptions.QUERY_TIMEOUT_SEC,
                        JdbcOptions.FETCH_SIZE,
                        JdbcOptions.QUERY,
                        JdbcSourceOptions.TABLE_NAME,
                        JdbcSourceOptions.WHERE_CONDITION,
                        StarRocksOptions.SCAN_FILTER,
                        StarRocksOptions.REQUEST_TABLET_SIZE,
                        StarRocksOptions.SCAN_BATCH_ROWS,
                        StarRocksOptions.SCAN_CONNECT_TIMEOUT_MS,
                        StarRocksOptions.SCAN_KEEP_ALIVE_MIN,
                        StarRocksOptions.SCAN_MEM_LIMIT,
                        JdbcOptions.MAX_RETRIES,
                        JdbcOptions.DATABASE,
                        JdbcOptions.TABLE)
                .build();
    }

    @Override
    public Class<? extends AresSource> getSourceClass() {
        return StarRocksFeBeSource.class;
    }
}
