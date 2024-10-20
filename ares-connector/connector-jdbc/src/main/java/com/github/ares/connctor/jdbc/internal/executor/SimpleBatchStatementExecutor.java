package com.github.ares.connctor.jdbc.internal.executor;

import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.connctor.jdbc.internal.converter.JdbcRowConverter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SimpleBatchStatementExecutor implements JdbcBatchStatementExecutor<AresRow> {
    private final StatementFactory statementFactory;
    private final AresRowType rowType;
    private final JdbcRowConverter converter;
    private transient PreparedStatement statement;

    public SimpleBatchStatementExecutor(StatementFactory statementFactory, AresRowType rowType, JdbcRowConverter converter) {
        this.statementFactory = statementFactory;
        this.rowType = rowType;
        this.converter = converter;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        statement = statementFactory.createStatement(connection);
    }

    @Override
    public void addToBatch(AresRow record) throws SQLException {
        converter.toExternal(rowType, record, statement);
        statement.addBatch();
    }

    @Override
    public void executeBatch() throws SQLException {
        statement.executeBatch();
        statement.clearBatch();
    }

    @Override
    public void closeStatements() throws SQLException {
        if (statement != null) {
            statement.close();
        }
    }
}
