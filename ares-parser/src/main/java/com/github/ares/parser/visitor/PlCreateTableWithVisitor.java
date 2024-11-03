package com.github.ares.parser.visitor;

import com.github.ares.com.fasterxml.jackson.core.JsonProcessingException;
import com.github.ares.com.fasterxml.jackson.core.type.TypeReference;
import com.github.ares.com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.ares.com.google.inject.Inject;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.common.utils.JsonUtils;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.datasource.SourceConfigPatcher;
import com.github.ares.parser.datasource.SourceConfigPatcherFactory;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalSetConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.github.ares.api.common.CommonOptions.CONNECTOR;
import static com.github.ares.api.common.CommonOptions.DATA_SOURCE;
import static com.github.ares.parser.utils.Constants.DEFAULT_DATASOURCE_PATCHER;

public class PlCreateTableWithVisitor {
    private static final String SOURCE_TYPE = "source";
    private static final String SINK_TYPE = "sink";

    @Inject
    private PlCreateSourceTableVisitor plCreateSourceTableVisitor;
    @Inject
    private PlCreateSinkTableVisitor plCreateSinkTableVisitor;

    private Map<String, LogicalCreateSinkTable> sinkTables;

    public void init(PlVisitorManager plVisitorManager) {
        this.sinkTables = plVisitorManager.getSourceSinkTable().getSinkTables();
        plCreateSourceTableVisitor.init(plVisitorManager.getSourceSinkTable().getSourceTables());
    }

    private PlCreateSourceTableVisitor getCreateSourceTableVisitor() {
        return plCreateSourceTableVisitor;
    }

    private PlCreateSinkTableVisitor getCreateSinkTableVisitor() {
        return plCreateSinkTableVisitor;
    }

    public List<LogicalOperation> visitCreateTableWith(PlSqlParser.Create_tableContext createTableContext,
                                                       PlSqlParser.Create_withContext createWithContext, List<LogicalOperation> setConfigs) {
        // create table ... with ...
        Map<String, Object> withOptions = visitCreateWithOptions(createWithContext, setConfigs);
        String tableType = (String) withOptions.get("type");
        if (tableType == null) {
            throw new IllegalArgumentException("The type of create table option must not be null");
        }

        String[] types = tableType.split(",");
        String standardType = "";
        if (types.length == 1) {
            if (SOURCE_TYPE.equalsIgnoreCase(types[0])) {
                standardType = SOURCE_TYPE;
            } else if (SINK_TYPE.equalsIgnoreCase(types[0])) {
                standardType = SINK_TYPE;
            }
        } else if (types.length == 2 && ((SOURCE_TYPE.equalsIgnoreCase(types[0]) && SINK_TYPE.equalsIgnoreCase(types[1])) ||
                (SOURCE_TYPE.equalsIgnoreCase(types[1]) && SINK_TYPE.equalsIgnoreCase(types[0])))) {
            standardType = SOURCE_TYPE + "," + SINK_TYPE;
        }

        List<LogicalOperation> result = new ArrayList<>();
        if (standardType.contains(SOURCE_TYPE)) {
            LogicalOperation operation = getCreateSourceTableVisitor().visitCreateSourceTable(createTableContext, withOptions);
            result.add(operation);
        }
        if (standardType.contains(SINK_TYPE)) {
            LogicalCreateSinkTable sinkTable = getCreateSinkTableVisitor().visitCreateSinkTable(createTableContext, withOptions);
            String sinkTableName = sinkTable.getTableName();
            if (sinkTables.containsKey(sinkTableName.toLowerCase())) {
                throw new ParseException(String.format("Sink table name exists: %s", sinkTableName));
            }
            sinkTables.put(sinkTableName.toLowerCase(), sinkTable);
            result.add(sinkTable);
        }
        if (!standardType.contains(SOURCE_TYPE) && !standardType.contains(SINK_TYPE)) {
            throw new IllegalArgumentException("The type of create table option must be 'source' or 'sink'");
        }
        return result;
    }

    private Map<String, Object> visitCreateWithOptions(PlSqlParser.Create_withContext create_withContext,
                                                       List<LogicalOperation> setConfigs) {
        Map<String, String> withOptions = new LinkedHashMap<>();
        PlSqlParser.Create_optionsContext createOptionsContext = create_withContext.create_options();
        visitCreateWithOptions(createOptionsContext, withOptions);
        String datasource = withOptions.get(DATA_SOURCE.key());
        if (StringUtils.isEmpty(withOptions.get(CONNECTOR.key())) && !StringUtils.isEmpty(datasource)) {
            Properties properties = new Properties();
            for (LogicalOperation operation : setConfigs) {
                if (operation instanceof LogicalSetConfig) {
                    LogicalSetConfig setConfig = (LogicalSetConfig) operation;
                    properties.put(setConfig.getKey(), setConfig.getValue());
                }
            }

            datasource = datasource.trim();
            SourceConfigPatcher sourceConfigPatcher = SourceConfigPatcherFactory.getSourceConfigPatcher(DEFAULT_DATASOURCE_PATCHER);
            Map<String, String> sourceConfig = sourceConfigPatcher.patchSourceConf(datasource, properties);
            if (sourceConfig != null) {
                withOptions.putAll(sourceConfig);
            }
        }
        withOptions.remove(DATA_SOURCE.key());

        Map<String, Object> resultOptions = new LinkedHashMap<>(withOptions.size());
        withOptions.forEach((key, value) -> {
            try {
                if (value != null && value.trim().startsWith("[") && value.endsWith("]")){
                    List<Object> arrayList = JsonUtils.OBJECT_MAPPER.readValue(value, new TypeReference<ArrayList<Object>>() {
                    });
                    resultOptions.put(key, arrayList);
                } else if (value != null && value.trim().startsWith("{") && value.endsWith("}")){
                    Map<String, Object> map = JsonUtils.OBJECT_MAPPER.readValue(value, new TypeReference<LinkedHashMap<String, Object>>() {
                    });
                    resultOptions.put(key, map);
                } else {
                    resultOptions.put(key, value);
                }
            } catch (JsonProcessingException e) {
                throw new AresException("Create table with option json parse error: " + value);
            }
        });
        return resultOptions;
    }

    private Object visitCreateOption(PlSqlParser.Option_Context option_context) {
        return null;
    }

    public void visitCreateWithOptions(PlSqlParser.Create_optionsContext create_optionsContext, Map<String, String> withOptions) {
        String invalidSynMessage = "Invalid syntax of create table: ";
        for (PlSqlParser.Option_Context option_context : create_optionsContext.option_()) {
            String optionKV = option_context.getText();
            int eqIdx = optionKV.indexOf("=");
            if (eqIdx < 0) {
                throw new ParseException(invalidSynMessage + optionKV);
            }
            String key = optionKV.substring(0, eqIdx);
            String value = optionKV.substring(eqIdx + 1);
            if (key.startsWith("'") && key.endsWith("'")) {
                key = key.substring(0, key.length() - 1).substring(1);
            } else {
                throw new ParseException(invalidSynMessage + key);
            }
            if (value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(0, value.length() - 1).substring(1);
            } else {
                throw new ParseException(invalidSynMessage + value);
            }
            withOptions.put(key, value);
        }
    }
}
