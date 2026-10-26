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
import com.github.ares.connector.doris.config.DorisOptions;
import com.github.ares.connector.doris.febe.DorisFeBeConfig;
import com.github.ares.connector.doris.febe.DorisFeBeSource;
import com.github.ares.connector.doris.util.DorisJdbcDefaults;
import com.github.ares.connector.jdbc.config.JdbcOptions;
import com.github.ares.connector.jdbc.config.JdbcSourceOptions;
import com.github.ares.connector.jdbc.internal.dialect.DatabaseIdentifier;
import com.google.auto.service.AutoService;
import java.io.Serializable;

@AutoService(Factory.class)
public class DorisSourceFactory extends JdbcSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.DORIS;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        DorisFeBeConfig.checkMode(context.getOptions());
        if (DorisFeBeConfig.isFeBe(context.getOptions())) {
            return () -> (AresSource<T, SplitT, StateT>) new DorisFeBeSource(context.getOptions());
        }
        if (!context.getOptions().getOptional(JdbcOptions.URL).isPresent()) {
            throw new AresException(
                    "Doris jdbc mode requires url, for example jdbc:mysql://fe_host:9030/database");
        }
        ReadonlyConfig options = DorisJdbcDefaults.apply(context.getOptions());
        return super.createSource(new TableSourceFactoryContext(options, context.getClassLoader()));
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(
                        DorisOptions.MODE,
                        DorisOptions.FE_NODES,
                        JdbcOptions.URL,
                        JdbcOptions.DRIVER,
                        JdbcOptions.USER,
                        JdbcOptions.PASSWORD,
                        JdbcOptions.QUERY_TIMEOUT_SEC,
                        JdbcOptions.FETCH_SIZE,
                        JdbcOptions.QUERY,
                        JdbcSourceOptions.TABLE_NAME,
                        JdbcSourceOptions.WHERE_CONDITION,
                        DorisOptions.SCAN_FILTER,
                        DorisOptions.REQUEST_TABLET_SIZE,
                        DorisOptions.SCAN_BATCH_ROWS,
                        DorisOptions.SCAN_CONNECT_TIMEOUT_MS,
                        DorisOptions.SCAN_KEEP_ALIVE_MIN,
                        DorisOptions.SCAN_MEM_LIMIT,
                        JdbcOptions.MAX_RETRIES,
                        JdbcOptions.DATABASE,
                        JdbcOptions.TABLE)
                .build();
    }

    @Override
    public Class<? extends AresSource> getSourceClass() {
        return DorisFeBeSource.class;
    }
}
