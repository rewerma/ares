package com.github.ares.connctor.jdbc.internal.executor;

import com.github.ares.api.table.type.AresRow;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class BufferedBatchStatementExecutor implements JdbcBatchStatementExecutor<AresRow> {
    private final JdbcBatchStatementExecutor<AresRow> statementExecutor;
    private final Function<AresRow, AresRow> valueTransform;
    private final List<AresRow> buffer = new ArrayList<>();

    public BufferedBatchStatementExecutor(JdbcBatchStatementExecutor<AresRow> statementExecutor, Function<AresRow, AresRow> valueTransform) {
        this.statementExecutor = statementExecutor;
        this.valueTransform = valueTransform;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        statementExecutor.prepareStatements(connection);
    }

    @Override
    public void addToBatch(AresRow record) throws SQLException {
        buffer.add(valueTransform.apply(record));
    }

    @Override
    public void executeBatch() throws SQLException {
        if (!buffer.isEmpty()) {
            for (AresRow row : buffer) {
                statementExecutor.addToBatch(row);
            }
            statementExecutor.executeBatch();
            buffer.clear();
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        if (!buffer.isEmpty()) {
            executeBatch();
        }
        statementExecutor.closeStatements();
    }
}
