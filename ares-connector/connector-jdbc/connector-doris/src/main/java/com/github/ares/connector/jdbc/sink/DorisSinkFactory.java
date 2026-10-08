package com.github.ares.connector.jdbc.sink;

import com.github.ares.api.common.CommonOptions;
import com.github.ares.api.common.SinkType;
import com.github.ares.api.table.connector.TableSink;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSinkFactory;
import com.github.ares.api.table.factory.TableSinkFactoryContext;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connector.doris.config.DorisOptions;
import com.github.ares.connector.doris.febe.DorisFeBeConfig;
import com.github.ares.connector.doris.febe.DorisFeBeSink;
import com.github.ares.connector.doris.util.DorisJdbcDefaults;
import com.github.ares.connector.jdbc.config.JdbcOptions;
import com.github.ares.connector.jdbc.internal.dialect.DatabaseIdentifier;
import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class DorisSinkFactory extends JdbcSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.DORIS;
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        DorisFeBeConfig.checkMode(options);
        if (DorisFeBeConfig.isFeBe(options) && isStreamLoad(options)) {
            return () -> new DorisFeBeSink(options, context.getCatalogTable());
        }
        if (DorisFeBeConfig.isFeBe(options) && !options.getOptional(JdbcOptions.URL).isPresent()) {
            throw new AresException(
                    "Doris febe mode executes "
                            + options.getOptional(CommonOptions.SINK_TYPE).orElse("this statement")
                            + " through JDBC. Configure url, for example jdbc:mysql://fe_host:9030/database");
        }
        if (!options.getOptional(JdbcOptions.URL).isPresent()) {
            throw new AresException(
                    "Doris jdbc mode requires url, for example jdbc:mysql://fe_host:9030/database");
        }
        ReadonlyConfig withDriver = DorisJdbcDefaults.apply(options);
        return super.createSink(
                new TableSinkFactoryContext(
                        context.getCatalogTable(), withDriver, context.getClassLoader()));
    }

    private static boolean isStreamLoad(ReadonlyConfig options) {
        String sinkType =
                options.getOptional(CommonOptions.SINK_TYPE).orElse(SinkType.INSERT.name());
        return SinkType.INSERT.name().equalsIgnoreCase(sinkType);
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
                        JdbcOptions.BATCH_SIZE,
                        JdbcOptions.MAX_RETRIES,
                        JdbcOptions.DATABASE,
                        JdbcOptions.TABLE,
                        JdbcOptions.TABLE_NAME,
                        DorisOptions.BATCH_MAX_BYTES,
                        DorisOptions.HTTP_SOCKET_TIMEOUT_MS,
                        DorisOptions.PARTIAL_UPDATE,
                        DorisOptions.STREAM_LOAD_PROPS)
                .build();
    }
}
