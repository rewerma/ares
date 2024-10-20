package com.github.ares.connctor.jdbc.internal.dialect.hive;

import com.github.ares.api.table.catalog.TableSchema;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.connctor.jdbc.exception.JdbcConnectorException;
import com.github.ares.connctor.jdbc.internal.converter.AbstractJdbcRowConverter;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class HiveJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String converterName() {
        return DatabaseIdentifier.HIVE;
    }

    @Override
    public PreparedStatement toExternal(
            AresRowType rowType, AresRow row, PreparedStatement statement) throws SQLException {
        throw new JdbcConnectorException(
                "The Hive jdbc connector don't support sink");
    }
}
