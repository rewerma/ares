package com.github.ares.parser.sqlparser.sparksql;

import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;
import com.github.ares.parser.model.SourceSinkTable;

/** Identifier text used as a name, without quoting characters. */
final class SqlIdentifier {
    private SqlIdentifier() {}

    static String plain(String text) {
        return text == null ? null : SourceSinkTable.plainName(text);
    }

    static String alias(SqlBaseParser.TableAliasContext alias) {
        if (alias == null || alias.strictIdentifier() == null) {
            return null;
        }
        return SourceSinkTable.plainName(alias.strictIdentifier().getText());
    }
}
