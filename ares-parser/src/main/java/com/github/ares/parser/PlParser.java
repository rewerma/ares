package com.github.ares.parser;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.org.antlr.v4.runtime.CharStream;
import com.github.ares.org.antlr.v4.runtime.CharStreams;
import com.github.ares.org.antlr.v4.runtime.CommonTokenStream;
import com.github.ares.parser.antlr4.CaseChangingCharStream;
import com.github.ares.parser.antlr4.CustomErrorListener;
import com.github.ares.parser.antlr4.plsql.PlSqlLexer;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.antlr4.plsql.PlSqlParser.Sql_scriptContext;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalProject;
import com.github.ares.parser.plan.LogicalSetConfig;
import com.github.ares.parser.plan.LogicalSetConfigs;
import com.github.ares.parser.visitor.PlVisitorManager;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.github.ares.parser.enums.OperationType.SET_CONFIGS;

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

            Sql_scriptContext sqlScriptContext = parser.sql_script();

            if (!lexerErrorListener.getErrors().isEmpty()) {
                throw new ParseException(String.join("\n", lexerErrorListener.getErrors()));
            }
            if (!parserErrorListener.getErrors().isEmpty()) {
                throw new ParseException(String.join("\n", parserErrorListener.getErrors()));
            }
            return sqlScriptContext;
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

    public List<String> parseDataSources(String script) {
        return parseDataSources(parse(script));
    }

    public List<String> parseDataSources(Sql_scriptContext sqlScriptContext) {
        LogicalProject logicalProject = parseToBaseBody(sqlScriptContext);
        List<LogicalSetConfig> setConfigs;
        Optional<LogicalOperation> logicalSetConfigsOp = logicalProject.getLogicalOperations().stream()
                .filter(op -> op.getOperationType() == SET_CONFIGS).findFirst();
        if (logicalSetConfigsOp.isPresent()) {
            LogicalSetConfigs logicalSetConfigs = (LogicalSetConfigs) logicalSetConfigsOp.get();
            setConfigs = logicalSetConfigs.getLogicalSetConfigs();
        } else {
            setConfigs = new ArrayList<>();
        }
        Set<String> setDataSources = new LinkedHashSet<>();
        for (LogicalSetConfig setConfig : setConfigs) {
            String key = setConfig.getKey();
            String[] subKeys = key.split("\\.");
            if (subKeys.length < 3) {
                continue;
            }
            if ("datasource".equals(subKeys[0])) {
                setDataSources.add(subKeys[1]);
            }
        }

        Set<String> usedDataSources = new LinkedHashSet<>();
        for (LogicalOperation logicalOperation : logicalProject.getLogicalOperations()) {
            if (logicalOperation.getOperationType() == OperationType.CREATE_SOURCE_TABLE) {
                LogicalCreateSourceTable createSourceTable = (LogicalCreateSourceTable) logicalOperation;
                Map<String, Object> options = createSourceTable.getOptions();
                if (options.containsKey("datasource")) {
                    usedDataSources.add((String) options.get("datasource"));
                }
            } else if (logicalOperation.getOperationType() == OperationType.CREATE_SINK_TABLE) {
                LogicalCreateSinkTable createSinkTable = (LogicalCreateSinkTable) logicalOperation;
                Map<String, Object> options = createSinkTable.getOptions();
                if (options.containsKey("datasource")) {
                    usedDataSources.add((String) options.get("datasource"));
                }
            }
        }
        setDataSources.forEach(usedDataSources::remove);

        return new ArrayList<>(usedDataSources);
    }
}
