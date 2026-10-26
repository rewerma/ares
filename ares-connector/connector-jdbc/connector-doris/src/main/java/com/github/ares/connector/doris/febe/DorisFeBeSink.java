package com.github.ares.connector.doris.febe;

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

public class DorisFeBeSink implements AresSink<AresRow, Void, Void, Void> {
    private static final long serialVersionUID = 1L;

    private final DorisFeBeConfig config;
    private final AresRowType rowType;

    public DorisFeBeSink(ReadonlyConfig options, CatalogTable catalogTable) {
        this.config = DorisFeBeConfig.from(options);
        if (catalogTable == null || catalogTable.getTableSchema() == null) {
            throw new AresException("Doris febe sink requires the incoming row schema");
        }
        this.rowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        if (rowType.getTotalFields() == 0) {
            throw new AresException("Doris febe sink row type has no columns");
        }
    }

    @Override
    public String getPluginName() {
        return "Doris";
    }

    @Override
    public SinkWriter<AresRow, Void, Void> createWriter(SinkWriter.Context context) {
        return new DorisFeBeSinkWriter(config, rowType, context.getIndexOfSubtask());
    }

    @Override
    public void truncateTable(String tableName) {
        if (StringUtils.isBlank(config.getJdbcUrl())) {
            throw new AresException(
                    "Doris febe TRUNCATE uses JDBC. Configure url, for example jdbc:mysql://fe_host:9030/database");
        }
        String sql = "TRUNCATE TABLE `" + config.getDatabase() + "`.`" + config.getTable() + "`";
        try {
            Class.forName(config.getDriverName());
            Properties properties = new Properties();
            properties.setProperty("user", config.getUsername());
            properties.setProperty("password", config.getPassword());
            JdbcConnectionConfig connectionConfig =
                    JdbcConnectionConfig.builder()
                            .dbType(DatabaseIdentifier.DORIS)
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
            throw new AresException("Truncate Doris table failed: " + sql, e);
        }
    }
}
