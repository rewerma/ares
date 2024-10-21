package com.github.ares.parser.visitor;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalUpdateSelectSQL;
import com.github.ares.parser.sqlparser.SQLParser;
import com.github.ares.parser.sqlparser.SQLParserFactory;
import com.github.ares.parser.sqlparser.SQLParserFactoryLoader;
import com.github.ares.parser.sqlparser.model.SQLHint;
import com.github.ares.parser.sqlparser.model.SQLUpdate;

import java.util.Locale;
import java.util.Map;

import static com.github.ares.parser.utils.PLParserUtil.setRepartition;
import static com.github.ares.parser.utils.PLParserUtil.setShowLine;

public class PlUpdateSQLVisitor {
    private Map<String, LogicalCreateSinkTable> sinkTables;

    private SQLParser sqlParser;

    public void init(PlVisitorManager plVisitorManager) {
        this.sinkTables = plVisitorManager.getSourceSinkTable().getSinkTables();
        SQLParserFactory sqlParserFactory = SQLParserFactoryLoader.getDefaultFactory();
        sqlParser = sqlParserFactory.getParser();
    }

    public LogicalOperation visitUpdateSQL(String originalSql, String updateSQL) {
        SQLUpdate sqlUpdate = sqlParser.parseUpdate(updateSQL);

        LogicalCreateSinkTable sinkTable = sinkTables.get(sqlUpdate.getTable().toLowerCase(Locale.ROOT));

        if (sinkTable == null) {
            throw new ParseException(String.format("Sink table name not exists: %s", sqlUpdate.getTable()));
        }
        String selectSQL = sqlUpdate.getSourceSql();
        LogicalUpdateSelectSQL updateSelectSQL = new LogicalUpdateSelectSQL();
        updateSelectSQL.setSinkTable(sinkTable);
        updateSelectSQL.setOriginSQL(originalSql);
        updateSelectSQL.setSelectSQL(selectSQL);
        updateSelectSQL.setUpdateItems(sqlUpdate.getUpdateColumns());
        updateSelectSQL.setWhereClause(sqlUpdate.getWhereClause());

        if (sqlUpdate.getHints() != null) {
            for (SQLHint hint : sqlUpdate.getHints()) {
                if ("show".equalsIgnoreCase(hint.getHintName())) {
                    setShowLine(hint, updateSelectSQL);
                } else if ("repartition".equalsIgnoreCase(hint.getHintName())) {
                    setRepartition(hint, updateSelectSQL);
                }
            }
        }

        return updateSelectSQL;
    }
}
