package com.github.ares.parser.sqlparser.sparksql;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;
import com.github.ares.parser.sqlparser.SQLParser;
import com.github.ares.parser.sqlparser.model.SQLDelete;
import com.github.ares.parser.sqlparser.model.SQLInsert;
import com.github.ares.parser.sqlparser.model.SQLMerge;
import com.github.ares.parser.sqlparser.model.SQLSelect;
import com.github.ares.parser.sqlparser.model.SQLTruncate;
import com.github.ares.parser.sqlparser.model.SQLUpdate;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static com.github.ares.parser.sqlparser.sparksql.CommonParser.UNSUPPORTED_EXP_MSG_WITH_PARAM;

public class SparkSqlParser implements SQLParser {


    @Override
    public SQLSelect parseSelect(String sql) {
        return SelectSqlParser.parseSelect(sql);
    }

    @Override
    public SQLInsert parseInsert(String sql) {
        return InsertSqlParser.parseInsert(sql);
    }

    @Override
    public SQLUpdate parseUpdate(String sql) {
        return UpdateSqlParser.parseUpdate(sql);
    }

    @Override
    public SQLDelete parseDelete(String sql) {
        return DeleteSqlParser.parseDelete(sql);
    }

    @Override
    public SQLMerge parseMerge(String sql) {
        return MergeSqlParser.parseMerge(sql);
    }

    @Override
    public SQLTruncate parseTruncate(String sql) {
        SQLTruncate sqlTruncate = new SQLTruncate();
        try (InputStream in = new ByteArrayInputStream(sql.getBytes(StandardCharsets.UTF_8))) {
            SqlBaseParser parser = CommonParser.parseSql(in);
            SqlBaseParser.StatementContext statementContext = parser.statement();
            if (!(statementContext instanceof SqlBaseParser.TruncateTableContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            SqlBaseParser.TruncateTableContext truncateTableContext = (SqlBaseParser.TruncateTableContext) statementContext;

            sqlTruncate.setTableName(truncateTableContext.multipartIdentifier().getText());
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException(e.getMessage(), e);
        }
        return sqlTruncate;
    }
}
