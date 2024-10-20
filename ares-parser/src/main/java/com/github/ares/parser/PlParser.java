package com.github.ares.parser;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.org.antlr.v4.runtime.CharStream;
import com.github.ares.org.antlr.v4.runtime.CharStreams;
import com.github.ares.org.antlr.v4.runtime.CommonTokenStream;
import com.github.ares.parser.antlr4.plsql.CaseChangingCharStream;
import com.github.ares.parser.antlr4.plsql.CustomErrorListener;
import com.github.ares.parser.antlr4.plsql.PlSqlLexer;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.antlr4.plsql.PlSqlParser.Sql_scriptContext;
import com.github.ares.parser.plan.LogicalProject;
import com.github.ares.parser.visitor.PlVisitorManager;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class PlParser {
    @Inject
    private PlVisitorManager visitorManager;

    public void init() {
        visitorManager.init();
    }

    private Sql_scriptContext parse(String script) {
        try (InputStream in = new ByteArrayInputStream(script.getBytes(StandardCharsets.UTF_8))) {
            return parse(in);
        } catch (Exception e) {
            throw new ParseException(e.getMessage(), e);
        }
    }

    private Sql_scriptContext parse(InputStream in) {
        try {
            CharStream s = CharStreams.fromStream(in);
            CaseChangingCharStream upper = new CaseChangingCharStream(s, true);

            CustomErrorListener lexerErrorListener = new CustomErrorListener();
            PlSqlLexer lexer = new PlSqlLexer(upper);
            lexer.removeErrorListeners();
            lexer.addErrorListener(lexerErrorListener);
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            PlSqlParser parser = new PlSqlParser(tokens);
            CustomErrorListener parserErrorListener = new CustomErrorListener();
            parser.removeErrorListeners();
            parser.addErrorListener(parserErrorListener);

            Sql_scriptContext sql_scriptContext = parser.sql_script();

            if (!lexerErrorListener.getErrors().isEmpty()) {
                throw new ParseException(String.join("\n", lexerErrorListener.getErrors()));
            }
            if (!parserErrorListener.getErrors().isEmpty()) {
                throw new ParseException(String.join("\n", parserErrorListener.getErrors()));
            }
            return sql_scriptContext;
        } catch (Exception e) {
            throw new ParseException(e.getMessage(), e);
        }
    }

    public LogicalProject parseToBaseBody(InputStream in) {
        return parseToBaseBody(parse(in));
    }

    public LogicalProject parseToBaseBody(String script) {
        return parseToBaseBody(parse(script));
    }

    private LogicalProject parseToBaseBody(Sql_scriptContext sqlScriptContext) {
        return visitorManager.getStatementVisitor().visitSqlScriptContext(sqlScriptContext);
    }
}
