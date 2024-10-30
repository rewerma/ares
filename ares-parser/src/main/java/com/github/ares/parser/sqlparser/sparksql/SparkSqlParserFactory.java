package com.github.ares.parser.sqlparser.sparksql;

import com.github.ares.parser.sqlparser.SQLParser;
import com.github.ares.parser.sqlparser.SQLParserFactory;
import com.google.auto.service.AutoService;

@AutoService(SQLParserFactory.class)
public class SparkSqlParserFactory implements SQLParserFactory {
    private final SparkSqlParser sparkSqlParser;

    public SparkSqlParserFactory() {
        this.sparkSqlParser = new SparkSqlParser();
    }

    @Override
    public String getType() {
        return "SparkSqlParser";
    }

    @Override
    public SQLParser getParser() {
        return sparkSqlParser;
    }
}
