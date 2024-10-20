package com.github.ares.parser.sqlparser.jsqlparser;

import com.github.ares.parser.sqlparser.SQLParser;
import com.github.ares.parser.sqlparser.SQLParserFactory;
import com.google.auto.service.AutoService;

@AutoService(SQLParserFactory.class)
public class JSQLParserFactory implements SQLParserFactory {
    private final JSQLParser jsqlParser;

    public JSQLParserFactory() {
        this.jsqlParser = new JSQLParser();
    }

    @Override
    public String getType() {
        return "JSQLParser";
    }

    @Override
    public SQLParser getParser() {
        return jsqlParser;
    }
}
