package com.github.ares.connctor.jdbc.internal.executor;

import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.RowKind;
import com.github.ares.common.exceptions.AresException;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class BufferReducedBatchStatementExecutor
        implements JdbcBatchStatementExecutor<AresRow> {
   private final JdbcBatchStatementExecutor<AresRow> upsertExecutor;
    private final JdbcBatchStatementExecutor<AresRow> deleteExecutor;
    private final Function<AresRow, AresRow> keyExtractor;
    private final Function<AresRow, AresRow> valueTransform;

    private final LinkedHashMap<AresRow, Pair<Boolean, AresRow>> buffer =
            new LinkedHashMap<>();

    public BufferReducedBatchStatementExecutor(JdbcBatchStatementExecutor<AresRow> upsertExecutor,
                                               JdbcBatchStatementExecutor<AresRow> deleteExecutor,
                                               Function<AresRow, AresRow> keyExtractor,
                                               Function<AresRow, AresRow> valueTransform) {
        this.upsertExecutor = upsertExecutor;
        this.deleteExecutor = deleteExecutor;
        this.keyExtractor = keyExtractor;
        this.valueTransform = valueTransform;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        upsertExecutor.prepareStatements(connection);
        deleteExecutor.prepareStatements(connection);
    }

    @Override
    public void addToBatch(AresRow record) throws SQLException {
        if (RowKind.UPDATE_BEFORE.equals(record.getRowKind())) {
            // do nothing
            return;
        }

        AresRow key = keyExtractor.apply(record);
        boolean changeFlag = changeFlag(record.getRowKind());
        AresRow value = valueTransform.apply(record);
        buffer.put(key, Pair.of(changeFlag, value));
    }

    @Override
    public void executeBatch() throws SQLException {
        Boolean preChangeFlag = null;
        Set<Map.Entry<AresRow, Pair<Boolean, AresRow>>> entrySet = buffer.entrySet();
        for (Map.Entry<AresRow, Pair<Boolean, AresRow>> entry : entrySet) {
            Boolean currentChangeFlag = entry.getValue().getKey();
            if (currentChangeFlag) {
                if (preChangeFlag != null && !preChangeFlag) {
                    deleteExecutor.executeBatch();
                }
                upsertExecutor.addToBatch(entry.getValue().getValue());
            } else {
                if (preChangeFlag != null && preChangeFlag) {
                    upsertExecutor.executeBatch();
                }
                deleteExecutor.addToBatch(entry.getKey());
            }
            preChangeFlag = currentChangeFlag;
        }

        if (preChangeFlag != null) {
            if (preChangeFlag) {
                upsertExecutor.executeBatch();
            } else {
                deleteExecutor.executeBatch();
            }
        }
        buffer.clear();
    }

    @Override
    public void closeStatements() throws SQLException {
        if (!buffer.isEmpty()) {
            executeBatch();
        }
        upsertExecutor.closeStatements();
        deleteExecutor.closeStatements();
    }

    private boolean changeFlag(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                return true;
            case DELETE:
            case UPDATE_BEFORE:
                return false;
            default:
                throw new AresException(
                        "Unsupported rowKind: " + rowKind);
        }
    }
}
