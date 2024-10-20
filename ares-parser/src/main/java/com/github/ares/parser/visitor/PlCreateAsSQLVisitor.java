package com.github.ares.parser.visitor;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalCreateTableAsSQL;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import com.github.ares.parser.sqlparser.SQLParser;
import com.github.ares.parser.sqlparser.SQLParserFactory;
import com.github.ares.parser.sqlparser.SQLParserFactoryLoader;
import com.github.ares.parser.sqlparser.model.SQLHint;
import com.github.ares.parser.sqlparser.model.SQLSelect;
import com.github.ares.parser.utils.PLParserUtil;

import java.util.Map;

import static com.github.ares.parser.utils.PLParserUtil.setRepartition;
import static com.github.ares.parser.utils.PLParserUtil.setShowLine;

public class PlCreateAsSQLVisitor {
    private Map<String, LogicalCreateSourceTable> sourceTables;

    private SQLParser sqlParser;

    public void init(PlVisitorManager plVisitorManager) {
        this.sourceTables = plVisitorManager.getSourceSinkTable().getSourceTables();
        SQLParserFactory sqlParserFactory = SQLParserFactoryLoader.getDefaultFactory();
        sqlParser = sqlParserFactory.getParser();
    }

    public LogicalOperation visitCreateInnerTable(String originalSql, String createSQL, String innerTableName) {
        if (sourceTables.containsKey(innerTableName.toLowerCase())) {
            throw new ParseException(String.format("Source table: %s exists, SQL: %s", innerTableName, createSQL));
        }
        sourceTables.put(innerTableName.toLowerCase(), null);
        int idx = createSQL.indexOf(innerTableName);
        idx = createSQL.toUpperCase().indexOf(" AS ", idx);
        String selectSQL = createSQL.substring(idx + 4);
        selectSQL = PLParserUtil.cleanSQL(selectSQL);


        LogicalCreateTableAsSQL createTableAsSQL = new LogicalCreateTableAsSQL();
        createTableAsSQL.setTableName(innerTableName);
        createTableAsSQL.setOriginSQL(originalSql);
        SQLSelect sqlSelect = sqlParser.parseSelect(selectSQL);
        createTableAsSQL.setSelectSQL(sqlSelect.getSourceSql());
        if (sqlSelect.getHints() != null) {
            for (SQLHint hint : sqlSelect.getHints()) {
                if ("cache".equalsIgnoreCase(hint.getHintName())) {
                    createTableAsSQL.setWithCache(true);
                } else if ("show".equalsIgnoreCase(hint.getHintName())) {
                    setShowLine(hint, createTableAsSQL);
                } else if ("repartition".equalsIgnoreCase(hint.getHintName())) {
                    setRepartition(hint, createTableAsSQL);
                }
            }

        }
        return createTableAsSQL;
    }
}
