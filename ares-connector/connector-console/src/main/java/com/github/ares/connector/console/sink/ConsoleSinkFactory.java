package com.github.ares.connector.console.sink;

import com.github.ares.api.table.connector.TableSink;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSinkFactory;
import com.github.ares.api.table.factory.TableSinkFactoryContext;
import com.github.ares.common.configuration.Option;
import com.github.ares.common.configuration.Options;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.configuration.utils.OptionRule;
import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class ConsoleSinkFactory implements TableSinkFactory {

    public static final Option<Boolean> LOG_PRINT_DATA =
            Options.key("log.print.data")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Flag to determine whether data should be printed in the logs.");

    public static final Option<Integer> LOG_PRINT_DELAY =
            Options.key("log.print.delay.ms")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Delay in milliseconds between printing each data item to the logs.");

    @Override
    public String factoryIdentifier() {
        return "console";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        return () ->
                new ConsoleSink(
                        context.getCatalogTable().getTableSchema().toPhysicalRowDataType(),
                        options);
    }
}
