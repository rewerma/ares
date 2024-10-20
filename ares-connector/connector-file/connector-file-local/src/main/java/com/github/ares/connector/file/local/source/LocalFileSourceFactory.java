package com.github.ares.connector.file.local.source;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.SourceSplit;
import com.github.ares.api.source.TableSource;
import com.github.ares.api.table.catalog.schema.TableSchemaOptions;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSourceFactory;
import com.github.ares.api.table.factory.TableSourceFactoryContext;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;
import com.github.ares.connector.file.config.FileFormat;
import com.github.ares.connector.file.config.FileSystemType;
import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.Arrays;

@AutoService(Factory.class)
public class LocalFileSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.LOCAL.getFileSystemPluginName();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
    TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (AresSource<T, SplitT, StateT>) new LocalFileSource(context.getOptions());
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(BaseSourceConfigOptions.TABLE_CONFIGS)
                .optional(BaseSourceConfigOptions.FILE_PATH)
                .optional(BaseSourceConfigOptions.FILE_FORMAT_TYPE)
                .optional(BaseSourceConfigOptions.ENCODING)
                .conditional(
                        BaseSourceConfigOptions.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        BaseSourceConfigOptions.FIELD_DELIMITER)
                .conditional(
                        BaseSourceConfigOptions.FILE_FORMAT_TYPE,
                        FileFormat.XML,
                        BaseSourceConfigOptions.XML_ROW_TAG,
                        BaseSourceConfigOptions.XML_USE_ATTR_FORMAT)
                .conditional(
                        BaseSourceConfigOptions.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT,
                                FileFormat.JSON,
                                FileFormat.EXCEL,
                                FileFormat.CSV,
                                FileFormat.XML),
                        TableSchemaOptions.SCHEMA)
                .optional(BaseSourceConfigOptions.PARSE_PARTITION_FROM_PATH)
                .optional(BaseSourceConfigOptions.DATE_FORMAT)
                .optional(BaseSourceConfigOptions.DATETIME_FORMAT)
                .optional(BaseSourceConfigOptions.TIME_FORMAT)
                .optional(BaseSourceConfigOptions.FILE_FILTER_PATTERN)
                .build();
    }

    @Override
    public Class<? extends AresSource> getSourceClass() {
        return LocalFileSource.class;
    }
}
