package com.github.ares.parser.paimon;

import com.github.ares.org.antlr.v4.runtime.ParserRuleContext;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;
import com.github.ares.parser.lake.LakeDdl;
import java.util.Map;

/** Paimon DDL helpers. Hudi and Iceberg use {@link LakeDdl} through the same rewrite. */
public final class PaimonDdl {
    private PaimonDdl() {}

    public static String rewritePrimaryKey(String sql) {
        return LakeDdl.rewritePrimaryKey(sql);
    }

    public static String sparkDdl(
            SqlBaseParser.CreateTableContext create,
            String source,
            Map<String, Object> paimonOptions,
            Map<String, String> extraProperties) {
        return LakeDdl.sparkDdl(create, source, paimonOptions, extraProperties);
    }

    public static Map<String, String> tableProperties(Map<String, Object> options) {
        return LakeDdl.tableProperties(options);
    }

    public static String slice(String source, ParserRuleContext context) {
        return LakeDdl.slice(source, context);
    }
}
