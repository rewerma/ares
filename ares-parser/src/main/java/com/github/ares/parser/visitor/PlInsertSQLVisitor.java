package com.github.ares.parser.visitor;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalInsertSelectSQL;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.sqlparser.SQLParser;
import com.github.ares.parser.sqlparser.SQLParserFactory;
import com.github.ares.parser.sqlparser.SQLParserFactoryLoader;
import com.github.ares.parser.sqlparser.model.SQLHint;
import com.github.ares.parser.sqlparser.model.SQLInsert;

import java.util.Locale;
import java.util.Map;

import static com.github.ares.parser.utils.PLParserUtil.setRepartition;
import static com.github.ares.parser.utils.PLParserUtil.setShowLine;

public class PlInsertSQLVisitor {
    private Map<String, LogicalCreateSinkTable> sinkTables;

    private SQLParser sqlParser;

    public void init(PlVisitorManager plVisitorManager) {
        this.sinkTables = plVisitorManager.getSourceSinkTable().getSinkTables();
        SQLParserFactory sqlParserFactory = SQLParserFactoryLoader.getDefaultFactory();
        sqlParser = sqlParserFactory.getParser();
    }

    public LogicalOperation visitInsertSQL(String originalSql, String insertSQL) {
        SQLInsert sqlInsert = sqlParser.parseInsert(insertSQL);

        LogicalInsertSelectSQL insertSelectSQL = new LogicalInsertSelectSQL();
        LogicalCreateSinkTable sinkTable = sinkTables.get(sqlInsert.getTable().toLowerCase(Locale.ROOT));
        if (sinkTable == null) {
            throw new ParseException(String.format("Sink table name not exists: %s", sqlInsert.getTable()));
        }
        insertSelectSQL.setSinkTable(sinkTable);
        insertSelectSQL.setOriginSQL(originalSql);
        insertSelectSQL.setSelectSQL(sqlInsert.getSourceSql());
        insertSelectSQL.setTargetColumns(sqlInsert.getColumns());

        if (sqlInsert.getHints() != null) {
            for (SQLHint hint : sqlInsert.getHints()) {
                if ("show".equalsIgnoreCase(hint.getHintName())) {
                    setShowLine(hint, insertSelectSQL);
                } else if ("repartition".equalsIgnoreCase(hint.getHintName())) {
                    setRepartition(hint, insertSelectSQL);
                }
            }
        }

        return insertSelectSQL;
    }
}
