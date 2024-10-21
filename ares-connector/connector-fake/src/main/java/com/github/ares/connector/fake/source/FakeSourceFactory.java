package com.github.ares.connector.fake.source;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.SourceSplit;
import com.github.ares.api.source.TableSource;
import com.github.ares.api.table.catalog.schema.TableSchemaOptions;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSourceFactory;
import com.github.ares.api.table.factory.TableSourceFactoryContext;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connector.fake.config.FakeOption;
import com.google.auto.service.AutoService;

import java.io.Serializable;

import static com.github.ares.connector.fake.config.FakeOption.ARRAY_SIZE;
import static com.github.ares.connector.fake.config.FakeOption.BIGINT_FAKE_MODE;
import static com.github.ares.connector.fake.config.FakeOption.BIGINT_TEMPLATE;
import static com.github.ares.connector.fake.config.FakeOption.BYTES_LENGTH;
import static com.github.ares.connector.fake.config.FakeOption.DATE_DAY_TEMPLATE;
import static com.github.ares.connector.fake.config.FakeOption.DATE_MONTH_TEMPLATE;
import static com.github.ares.connector.fake.config.FakeOption.DATE_YEAR_TEMPLATE;
import static com.github.ares.connector.fake.config.FakeOption.DOUBLE_FAKE_MODE;
import static com.github.ares.connector.fake.config.FakeOption.DOUBLE_TEMPLATE;
import static com.github.ares.connector.fake.config.FakeOption.FLOAT_FAKE_MODE;
import static com.github.ares.connector.fake.config.FakeOption.FLOAT_TEMPLATE;
import static com.github.ares.connector.fake.config.FakeOption.INT_FAKE_MODE;
import static com.github.ares.connector.fake.config.FakeOption.INT_TEMPLATE;
import static com.github.ares.connector.fake.config.FakeOption.MAP_SIZE;
import static com.github.ares.connector.fake.config.FakeOption.ROWS;
import static com.github.ares.connector.fake.config.FakeOption.ROW_NUM;
import static com.github.ares.connector.fake.config.FakeOption.SMALLINT_FAKE_MODE;
import static com.github.ares.connector.fake.config.FakeOption.SMALLINT_TEMPLATE;
import static com.github.ares.connector.fake.config.FakeOption.SPLIT_NUM;
import static com.github.ares.connector.fake.config.FakeOption.SPLIT_READ_INTERVAL;
import static com.github.ares.connector.fake.config.FakeOption.STRING_FAKE_MODE;
import static com.github.ares.connector.fake.config.FakeOption.STRING_TEMPLATE;
import static com.github.ares.connector.fake.config.FakeOption.TABLES_CONFIGS;
import static com.github.ares.connector.fake.config.FakeOption.TIME_HOUR_TEMPLATE;
import static com.github.ares.connector.fake.config.FakeOption.TIME_MINUTE_TEMPLATE;
import static com.github.ares.connector.fake.config.FakeOption.TIME_SECOND_TEMPLATE;
import static com.github.ares.connector.fake.config.FakeOption.TINYINT_FAKE_MODE;
import static com.github.ares.connector.fake.config.FakeOption.TINYINT_TEMPLATE;

@AutoService(Factory.class)
public class FakeSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "fake";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(TABLES_CONFIGS)
                .optional(TableSchemaOptions.SCHEMA)
                .optional(STRING_FAKE_MODE)
                .conditional(STRING_FAKE_MODE, FakeOption.FakeMode.TEMPLATE, STRING_TEMPLATE)
                .optional(TINYINT_FAKE_MODE)
                .conditional(TINYINT_FAKE_MODE, FakeOption.FakeMode.TEMPLATE, TINYINT_TEMPLATE)
                .optional(SMALLINT_FAKE_MODE)
                .conditional(SMALLINT_FAKE_MODE, FakeOption.FakeMode.TEMPLATE, SMALLINT_TEMPLATE)
                .optional(INT_FAKE_MODE)
                .conditional(INT_FAKE_MODE, FakeOption.FakeMode.TEMPLATE, INT_TEMPLATE)
                .optional(BIGINT_FAKE_MODE)
                .conditional(BIGINT_FAKE_MODE, FakeOption.FakeMode.TEMPLATE, BIGINT_TEMPLATE)
                .optional(FLOAT_FAKE_MODE)
                .conditional(FLOAT_FAKE_MODE, FakeOption.FakeMode.TEMPLATE, FLOAT_TEMPLATE)
                .optional(DOUBLE_FAKE_MODE)
                .conditional(DOUBLE_FAKE_MODE, FakeOption.FakeMode.TEMPLATE, DOUBLE_TEMPLATE)
                .optional(
                        ROWS,
                        ROW_NUM,
                        SPLIT_NUM,
                        SPLIT_READ_INTERVAL,
                        MAP_SIZE,
                        ARRAY_SIZE,
                        BYTES_LENGTH,
                        DATE_YEAR_TEMPLATE,
                        DATE_MONTH_TEMPLATE,
                        DATE_DAY_TEMPLATE,
                        TIME_HOUR_TEMPLATE,
                        TIME_MINUTE_TEMPLATE,
                        TIME_SECOND_TEMPLATE)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
    TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (AresSource<T, SplitT, StateT>) new FakeSource(context.getOptions());
    }

    @Override
    public Class<? extends AresSource> getSourceClass() {
        return FakeSource.class;
    }
}
