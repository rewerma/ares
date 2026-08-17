package com.github.ares.connector.file.local.sink;

import com.github.ares.api.sink.SinkReplaceNameConstant;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.TableIdentifier;
import com.github.ares.api.table.connector.TableSink;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSinkFactory;
import com.github.ares.api.table.factory.TableSinkFactoryContext;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.com.typesafe.config.ConfigValueFactory;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connector.file.config.BaseSinkConfig;
import com.github.ares.connector.file.config.FileFormat;
import com.github.ares.connector.file.config.FileSystemType;
import com.github.ares.connector.file.sink.commit.FileAggregatedCommitInfo;
import com.github.ares.connector.file.sink.commit.FileCommitInfo;
import com.github.ares.connector.file.sink.state.FileSinkState;
import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class LocalFileSinkFactory
        implements TableSinkFactory<
                AresRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo> {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.LOCAL.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(BaseSinkConfig.FILE_PATH)
                .optional(BaseSinkConfig.FILE_FORMAT_TYPE)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        BaseSinkConfig.ROW_DELIMITER,
                        BaseSinkConfig.FIELD_DELIMITER,
                        BaseSinkConfig.TXT_COMPRESS,
                        BaseSinkConfig.ENABLE_HEADER_WRITE)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.CSV,
                        BaseSinkConfig.TXT_COMPRESS,
                        BaseSinkConfig.ENABLE_HEADER_WRITE)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.JSON,
                        BaseSinkConfig.TXT_COMPRESS)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.ORC,
                        BaseSinkConfig.ORC_COMPRESS)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.PARQUET,
                        BaseSinkConfig.PARQUET_COMPRESS)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.XML,
                        BaseSinkConfig.XML_USE_ATTR_FORMAT)
                .optional(BaseSinkConfig.CUSTOM_FILENAME)
                .conditional(
                        BaseSinkConfig.CUSTOM_FILENAME,
                        true,
                        BaseSinkConfig.FILE_NAME_EXPRESSION,
                        BaseSinkConfig.FILENAME_TIME_FORMAT)
                .optional(BaseSinkConfig.HAVE_PARTITION)
                .conditional(
                        BaseSinkConfig.HAVE_PARTITION,
                        true,
                        BaseSinkConfig.PARTITION_BY,
                        BaseSinkConfig.PARTITION_DIR_EXPRESSION,
                        BaseSinkConfig.IS_PARTITION_FIELD_WRITE_IN_FILE)
                .optional(BaseSinkConfig.SINK_COLUMNS)
                .optional(BaseSinkConfig.IS_ENABLE_TRANSACTION)
                .optional(BaseSinkConfig.DATE_FORMAT)
                .optional(BaseSinkConfig.DATETIME_FORMAT)
                .optional(BaseSinkConfig.TIME_FORMAT)
                .build();
    }

    @Override
    public TableSink<AresRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo> createSink(
            TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();

        ReadonlyConfig finalReadonlyConfig =
                generateCurrentReadonlyConfig(readonlyConfig, catalogTable);
        return () -> new LocalFileSink(finalReadonlyConfig, catalogTable);
    }

    // replace the table name in sink config's path
    public ReadonlyConfig generateCurrentReadonlyConfig(
            ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        // Copy the config to avoid modifying the original config
        Config config = readonlyConfig.toConfig();

        if (config.hasPath(BaseSinkConfig.FILE_PATH.key())) {
            String replacedPath =
                    replaceCatalogTableInPath(
                            config.getString(BaseSinkConfig.FILE_PATH.key()), catalogTable);
            config =
                    config.withValue(
                            BaseSinkConfig.FILE_PATH.key(),
                            ConfigValueFactory.fromAnyRef(replacedPath));
        }

        if (config.hasPath(BaseSinkConfig.TMP_PATH.key())) {
            String replacedPath =
                    replaceCatalogTableInPath(
                            config.getString(BaseSinkConfig.TMP_PATH.key()), catalogTable);
            config =
                    config.withValue(
                            BaseSinkConfig.TMP_PATH.key(),
                            ConfigValueFactory.fromAnyRef(replacedPath));
        }

        return ReadonlyConfig.fromConfig(config);
    }

    public String replaceCatalogTableInPath(String originString, CatalogTable catalogTable) {
        String path = originString;
        TableIdentifier tableIdentifier = catalogTable.getTableId();
        if (tableIdentifier != null) {
            if (tableIdentifier.getDatabaseName() != null) {
                path =
                        path.replace(
                                SinkReplaceNameConstant.REPLACE_DATABASE_NAME_KEY,
                                tableIdentifier.getDatabaseName());
            }
            if (tableIdentifier.getSchemaName() != null) {
                path =
                        path.replace(
                                SinkReplaceNameConstant.REPLACE_SCHEMA_NAME_KEY,
                                tableIdentifier.getSchemaName());
            }
            if (tableIdentifier.getTableName() != null) {
                path =
                        path.replace(
                                SinkReplaceNameConstant.REPLACE_TABLE_NAME_KEY,
                                tableIdentifier.getTableName());
            }
        }
        return path;
    }
}
