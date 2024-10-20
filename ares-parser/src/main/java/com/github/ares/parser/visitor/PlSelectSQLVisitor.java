package com.github.ares.parser.visitor;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.model.Argument;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalSelectIntoSQL;
import com.github.ares.parser.plan.LogicalSelectSQL;
import com.github.ares.parser.sqlparser.SQLParser;
import com.github.ares.parser.sqlparser.SQLParserFactory;
import com.github.ares.parser.sqlparser.SQLParserFactoryLoader;
import com.github.ares.parser.sqlparser.model.SQLSelect;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.github.ares.parser.utils.PLParserUtil.getTargetType;

public class PlSelectSQLVisitor {
    private SQLParser sqlParser;

    public void init() {
        SQLParserFactory sqlParserFactory = SQLParserFactoryLoader.getDefaultFactory();
        sqlParser = sqlParserFactory.getParser();
    }

    public LogicalOperation visitSelectSQL(String originalSql, String selectSQL, Map<String, PlType> declaredParams) {
        SQLSelect sqlSelect = sqlParser.parseSelect(selectSQL);
        if (sqlSelect.getIntoParams() == null) {
            LogicalSelectSQL selectSQLModel = new LogicalSelectSQL();
            selectSQLModel.setSql(sqlSelect.getSourceSql());
            selectSQLModel.setOriginSQL(originalSql);
            return selectSQLModel;
        } else {
            Map<String, PlType> intoParamsWithType = new LinkedHashMap<>();
            for (String intoParam : sqlSelect.getIntoParams()) {
                if (declaredParams.containsKey(intoParam)) {
                    intoParamsWithType.put(intoParam, declaredParams.get(intoParam));
                } else {
                    throw new ParseException(String.format("Parameter %s undefined", intoParam));
                }
            }
            List<Argument> intoParamList = new ArrayList<>();
            intoParamsWithType.forEach((param, type) -> {
                Argument argument = new Argument(param, type);
                intoParamList.add(argument);
            });

            LogicalSelectIntoSQL selectIntoSQL = new LogicalSelectIntoSQL();
            selectIntoSQL.setOriginSQL(originalSql);
            selectIntoSQL.setSql(sqlSelect.getSourceSql());
            selectIntoSQL.setIntoParams(intoParamList);
            return selectIntoSQL;
        }
    }
}
