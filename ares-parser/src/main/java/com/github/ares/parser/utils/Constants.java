package com.github.ares.parser.utils;

public class Constants {
    private Constants() {
    }

    public static final String DEFAULT_DATASOURCE_PATCHER = "DEFAULT";

    public static final String SQL_SINGLE_LINE_COMMENT = "--";

    public static final String SQL_MULTI_LINE_COMMENT_PREFIX = "/*";

    public static final String SQL_CONFIG_PREFIX = SQL_MULTI_LINE_COMMENT_PREFIX + " config";

    public static final String SQL_MULTI_LINE_COMMENT_SUFFIX = "*/";
}
