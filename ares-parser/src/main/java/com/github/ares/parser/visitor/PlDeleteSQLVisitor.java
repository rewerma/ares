package com.github.ares.parser.visitor;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalDeleteSelectSQL;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.sqlparser.SQLParser;
import com.github.ares.parser.sqlparser.SQLParserFactory;
import com.github.ares.parser.sqlparser.SQLParserFactoryLoader;
import com.github.ares.parser.sqlparser.model.SQLDelete;
import com.github.ares.parser.sqlparser.model.SQLHint;

import java.util.Map;

import static com.github.ares.parser.utils.PLParserUtil.setRepartition;
import static com.github.ares.parser.utils.PLParserUtil.setShowLine;

public class PlDeleteSQLVisitor {
    private Map<String, LogicalCreateSinkTable> sinkTables;

    private SQLParser sqlParser;

    public void init(PlVisitorManager plVisitorManager) {
        this.sinkTables = plVisitorManager.getSourceSinkTable().getSinkTables();
        SQLParserFactory sqlParserFactory = SQLParserFactoryLoader.getDefaultFactory();
        sqlParser = sqlParserFactory.getParser();
    }

    public LogicalOperation visitDeleteSQL(String originalSql, String deleteSQL) {
        SQLDelete sqlDelete = sqlParser.parseDelete(deleteSQL);

        LogicalCreateSinkTable sinkTable = sinkTables.get(sqlDelete.getTable().toLowerCase());
        if (sinkTable == null) {
            throw new ParseException(String.format("Sink table name not exists: %s", sqlDelete.getTable()));
        }

        String selectSQL = sqlDelete.getSourceSql();
        LogicalDeleteSelectSQL deleteSelectSQL = new LogicalDeleteSelectSQL();
        deleteSelectSQL.setSinkTable(sinkTable);
        deleteSelectSQL.setOriginSQL(originalSql);
        deleteSelectSQL.setSelectSQL(selectSQL);
        deleteSelectSQL.setWhereClause(sqlDelete.getWhereClause());

        if (sqlDelete.getHints() != null) {
            for (SQLHint hint : sqlDelete.getHints()) {
                if ("show".equalsIgnoreCase(hint.getHintName())) {
                    setShowLine(hint, deleteSelectSQL);
                } else if ("repartition".equalsIgnoreCase(hint.getHintName())) {
                    setRepartition(hint, deleteSelectSQL);
                }
            }
        }

        return deleteSelectSQL;
    }

}
