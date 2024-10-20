package com.github.ares.parser.visitor;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalMergeIntoSQL;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import com.github.ares.parser.sqlparser.SQLParser;
import com.github.ares.parser.sqlparser.SQLParserFactory;
import com.github.ares.parser.sqlparser.SQLParserFactoryLoader;
import com.github.ares.parser.sqlparser.model.SQLHint;
import com.github.ares.parser.sqlparser.model.SQLInsert;
import com.github.ares.parser.sqlparser.model.SQLMerge;
import com.github.ares.parser.sqlparser.model.SQLUpdate;

import java.util.Map;

import static com.github.ares.parser.utils.PLParserUtil.setRepartition;
import static com.github.ares.parser.utils.PLParserUtil.setShowLine;

public class PlMergeSQLVisitor {
    private Map<String, LogicalCreateSourceTable> sourceTables;
    private Map<String, LogicalCreateSinkTable> sinkTables;

    private SQLParser sqlParser;

    public void init(PlVisitorManager plVisitorManager) {
        this.sourceTables = plVisitorManager.getSourceSinkTable().getSourceTables();
        this.sinkTables = plVisitorManager.getSourceSinkTable().getSinkTables();
        SQLParserFactory sqlParserFactory = SQLParserFactoryLoader.getDefaultFactory();
        sqlParser = sqlParserFactory.getParser();
    }

    public LogicalOperation visitMergeSQL(String originalSql, String mergeSQL) {
        SQLMerge sqlMerge = sqlParser.parseMerge(mergeSQL);

        LogicalCreateSinkTable sinkTable = sinkTables.get(sqlMerge.getTable().toLowerCase());
        if (sinkTable == null) {
            throw new ParseException(String.format("sink table name not exists: %s", sqlMerge.getTable()));
        }
        LogicalCreateSourceTable sourceTable = sourceTables.get(sqlMerge.getTable().toLowerCase());
        if (sourceTable == null) {
            throw new ParseException(String.format("the target table '%s' must be a source table in merge sql", sqlMerge.getTable()));
        }

        LogicalMergeIntoSQL mergeIntoSQL = new LogicalMergeIntoSQL();
        mergeIntoSQL.setOriginSQL(originalSql);
        mergeIntoSQL.setSinkTable(sinkTable);

        mergeIntoSQL.setAllWaitCriteria(sqlMerge.getAllWhereClause());
        SQLInsert sqlInsert = sqlMerge.getSqlInsert();
        if (sqlInsert != null) {
            mergeIntoSQL.setInsertColumns(sqlInsert.getColumns());
            mergeIntoSQL.setInsertSourceSql(sqlInsert.getSourceSql());
        }
        SQLUpdate sqlUpdate = sqlMerge.getSqlUpdate();
        if (sqlUpdate != null) {
            mergeIntoSQL.setUpdateColumns(sqlUpdate.getUpdateColumns());
            mergeIntoSQL.setUpdateValues(sqlUpdate.getUpdateValues());
            mergeIntoSQL.setUpdateWhereClause(sqlUpdate.getWhereClause());
            mergeIntoSQL.setUpdateWhereSelectItems(sqlMerge.getSqlUpdate().getSelectWhereItems());
            mergeIntoSQL.setUpdateSourceSql(sqlUpdate.getSourceSql());
        }

        if (sqlMerge.getHints() != null) {
            for (SQLHint hint : sqlMerge.getHints()) {
                if ("show".equalsIgnoreCase(hint.getHintName())) {
                    setShowLine(hint, mergeIntoSQL);
                } else if ("repartition".equalsIgnoreCase(hint.getHintName())) {
                    setRepartition(hint, mergeIntoSQL);
                }
            }
        }

        return mergeIntoSQL;
    }
}
