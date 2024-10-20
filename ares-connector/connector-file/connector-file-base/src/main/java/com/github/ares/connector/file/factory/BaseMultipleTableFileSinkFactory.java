package com.github.ares.connector.file.factory;

import com.github.ares.api.sink.SinkReplaceNameConstant;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.TableIdentifier;
import com.github.ares.api.table.factory.TableSinkFactory;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.com.typesafe.config.ConfigValueFactory;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.connector.file.config.BaseSinkConfig;
import com.github.ares.connector.file.sink.commit.FileAggregatedCommitInfo;
import com.github.ares.connector.file.sink.commit.FileCommitInfo;
import com.github.ares.connector.file.sink.state.FileSinkState;

public abstract class BaseMultipleTableFileSinkFactory
        implements TableSinkFactory<
                AresRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo> {

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
