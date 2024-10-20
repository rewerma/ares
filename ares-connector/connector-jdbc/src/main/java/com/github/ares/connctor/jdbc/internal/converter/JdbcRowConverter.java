package com.github.ares.connctor.jdbc.internal.converter;

import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Converter that is responsible to convert between JDBC object and Ares data structure {@link AresRow}.
 */
public interface JdbcRowConverter extends Serializable {

    /**
     * Convert data retrieved from {@link ResultSet} to internal {@link AresRow}.
     *
     * @param rs ResultSet from JDBC
     */
    AresRow toInternal(ResultSet rs, AresRowType typeInfo) throws SQLException;

    PreparedStatement toExternal(
            AresRowType rowType, AresRow row, PreparedStatement statement)
            throws SQLException;
}
