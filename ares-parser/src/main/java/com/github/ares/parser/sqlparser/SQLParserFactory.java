package com.github.ares.parser.sqlparser;

public interface SQLParserFactory {
    String getType();

    SQLParser getParser();
}
