package com.github.ares.parser.visitor;


import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalTruncateSQL;
import com.github.ares.parser.sqlparser.SQLParser;
import com.github.ares.parser.sqlparser.SQLParserFactory;
import com.github.ares.parser.sqlparser.SQLParserFactoryLoader;
import com.github.ares.parser.sqlparser.model.SQLTruncate;

import java.util.Locale;
import java.util.Map;

public class PlTruncateSQLVisitor {
    private Map<String, LogicalCreateSinkTable> sinkTables;

    private SQLParser sqlParser;

    public void init(PlVisitorManager plVisitorManager) {
        this.sinkTables = plVisitorManager.getSourceSinkTable().getSinkTables();
        SQLParserFactory sqlParserFactory = SQLParserFactoryLoader.getDefaultFactory();
        sqlParser = sqlParserFactory.getParser();
    }

    public LogicalOperation visitTruncateSQL(String truncateSQL) {
        SQLTruncate sqlTruncate = sqlParser.parseTruncate(truncateSQL);

        LogicalCreateSinkTable sinkTable = sinkTables.get(sqlTruncate.getTableName().toLowerCase(Locale.ROOT));
        if (sinkTable == null ) {
            throw new ParseException(String.format("sink table name not exists: %s", sqlTruncate.getTableName()));
        }

        LogicalTruncateSQL truncSQL = new LogicalTruncateSQL();
        truncSQL.setOriginSQL(truncateSQL);
        truncSQL.setSinkTable(sinkTable);
        return truncSQL;
    }
}
