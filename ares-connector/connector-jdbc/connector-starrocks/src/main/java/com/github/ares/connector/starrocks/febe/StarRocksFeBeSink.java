package com.github.ares.connector.starrocks.febe;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.sink.SinkWriter;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connector.jdbc.config.JdbcConnectionConfig;
import com.github.ares.connector.jdbc.internal.dialect.DatabaseIdentifier;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

public class StarRocksFeBeSink implements AresSink<AresRow, Void, Void, Void> {
    private static final long serialVersionUID = 1L;

    private final StarRocksFeBeConfig config;
    private final AresRowType rowType;

    public StarRocksFeBeSink(ReadonlyConfig options, CatalogTable catalogTable) {
        this.config = StarRocksFeBeConfig.from(options);
        if (catalogTable == null || catalogTable.getTableSchema() == null) {
            throw new AresException("StarRocks febe sink requires the incoming row schema");
        }
        this.rowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        if (rowType.getTotalFields() == 0) {
            throw new AresException("StarRocks febe sink row type has no columns");
        }
    }

    @Override
    public String getPluginName() {
        return "StarRocks";
    }

    @Override
    public SinkWriter<AresRow, Void, Void> createWriter(SinkWriter.Context context) {
        return new StarRocksFeBeSinkWriter(config, rowType, context.getIndexOfSubtask());
    }

    @Override
    public void truncateTable(String tableName) {
        if (StringUtils.isBlank(config.getJdbcUrl())) {
            throw new AresException(
                    "StarRocks febe TRUNCATE uses JDBC. Configure url, for example jdbc:mysql://fe_host:9030/database");
        }
        String sql = "TRUNCATE TABLE `" + config.getDatabase() + "`.`" + config.getTable() + "`";
        try {
            Class.forName(config.getDriverName());
            Properties properties = new Properties();
            properties.setProperty("user", config.getUsername());
            properties.setProperty("password", config.getPassword());
            JdbcConnectionConfig connectionConfig =
                    JdbcConnectionConfig.builder()
                            .dbType(DatabaseIdentifier.STARROCKS)
                            .url(config.getJdbcUrl())
                            .driverName(config.getDriverName())
                            .username(config.getUsername())
                            .password(config.getPassword())
                            .build();
            try (Connection connection =
                    DriverManager.getConnection(config.getJdbcUrl(), properties)) {
                connectionConfig.applySessionSettings(connection);
                try (Statement statement = connection.createStatement()) {
                    statement.execute(sql);
                }
            }
        } catch (Exception e) {
            throw new AresException("Truncate StarRocks table failed: " + sql, e);
        }
    }
}
