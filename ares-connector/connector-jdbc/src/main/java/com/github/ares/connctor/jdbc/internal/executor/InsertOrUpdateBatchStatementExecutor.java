package com.github.ares.connctor.jdbc.internal.executor;

import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connctor.jdbc.internal.converter.JdbcRowConverter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;

public class InsertOrUpdateBatchStatementExecutor
        implements JdbcBatchStatementExecutor<AresRow> {
    private final StatementFactory existStmtFactory;
    private final StatementFactory insertStmtFactory;
    private final StatementFactory updateStmtFactory;
    private final AresRowType keyRowType;
    private final Function<AresRow, AresRow> keyExtractor;
    private final AresRowType valueRowType;
    private final JdbcRowConverter rowConverter;
    private transient PreparedStatement existStatement;
    private transient PreparedStatement insertStatement;
    private transient PreparedStatement updateStatement;
    private transient Boolean preExistFlag;
    private transient boolean submitted;

    public InsertOrUpdateBatchStatementExecutor(StatementFactory existStmtFactory, StatementFactory insertStmtFactory,
                                                StatementFactory updateStmtFactory, AresRowType keyRowType, Function<AresRow, AresRow> keyExtractor,
                                                AresRowType valueRowType, JdbcRowConverter rowConverter) {
        this.existStmtFactory = existStmtFactory;
        this.insertStmtFactory = insertStmtFactory;
        this.updateStmtFactory = updateStmtFactory;
        this.keyRowType = keyRowType;
        this.keyExtractor = keyExtractor;
        this.valueRowType = valueRowType;
        this.rowConverter = rowConverter;
    }

    public InsertOrUpdateBatchStatementExecutor(
            StatementFactory insertStmtFactory,
            StatementFactory updateStmtFactory,
            AresRowType valueRowType,
            JdbcRowConverter rowConverter) {
        this(null, insertStmtFactory, updateStmtFactory, null, null, valueRowType, rowConverter);
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        if (upsertMode()) {
            existStatement = existStmtFactory.createStatement(connection);
        }
        insertStatement = insertStmtFactory.createStatement(connection);
        updateStatement = updateStmtFactory.createStatement(connection);
    }

    @Override
    public void addToBatch(AresRow record) throws SQLException {
        boolean exist = existRow(record);
        if (exist) {
            if (preExistFlag != null && !preExistFlag) {
                insertStatement.executeBatch();
                insertStatement.clearBatch();
            }
            rowConverter.toExternal(valueRowType, record, updateStatement);
            updateStatement.addBatch();
        } else {
            if (preExistFlag != null && preExistFlag) {
                updateStatement.executeBatch();
                updateStatement.clearBatch();
            }
            rowConverter.toExternal(valueRowType, record, insertStatement);
            insertStatement.addBatch();
        }

        preExistFlag = exist;
        submitted = false;
    }

    @Override
    public void executeBatch() throws SQLException {
        if (preExistFlag != null) {
            if (preExistFlag) {
                updateStatement.executeBatch();
                updateStatement.clearBatch();
            } else {
                insertStatement.executeBatch();
                insertStatement.clearBatch();
            }
        }
        submitted = true;
    }

    @Override
    public void closeStatements() throws SQLException {
        if (!submitted) {
            executeBatch();
        }
        for (PreparedStatement statement :
                Arrays.asList(existStatement, insertStatement, updateStatement)) {
            if (statement != null) {
                statement.close();
            }
        }
    }

    private boolean upsertMode() {
        return existStmtFactory != null;
    }

    private boolean existRow(AresRow record) throws SQLException {
        if (upsertMode()) {
            return exist(keyExtractor.apply(record));
        }
        switch (record.getRowKind()) {
            case INSERT:
                return false;
            case UPDATE_AFTER:
                return true;
            default:
                throw new AresException(
                        "unsupported row kind: " + record.getRowKind());
        }
    }

    private boolean exist(AresRow pk) throws SQLException {
        rowConverter.toExternal(keyRowType, pk, existStatement);
        try (ResultSet resultSet = existStatement.executeQuery()) {
            return resultSet.next();
        }
    }
}
