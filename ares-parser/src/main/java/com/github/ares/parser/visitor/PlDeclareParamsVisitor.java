package com.github.ares.parser.visitor;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.common.utils.Tuple2;
import com.github.ares.parser.model.Argument;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalDeclareParams;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.utils.PLParserUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.github.ares.parser.utils.PLParserUtil.getTargetType;

public class PlDeclareParamsVisitor {
    @Inject
    private PlDataTypePrecisionVisitor plDataTypePrecisionVisitor;

    public LogicalOperation visitDeclareParams(PlSqlParser.Seq_of_declare_specsContext declare_specsContext, Map<String, PlType> declaredParams) {
        Map<String, String> defaultValues = new LinkedHashMap<>();
        if (declare_specsContext != null) {
            List<PlSqlParser.Declare_specContext> declare_specContexts = declare_specsContext.declare_spec();
            for (PlSqlParser.Declare_specContext declare_specContext : declare_specContexts) {
                PlSqlParser.Variable_declarationContext variable_declarationContext = declare_specContext.variable_declaration();
                if (variable_declarationContext.type_spec().datatype() == null) {
                    throw new ParseException(String.format("Unsupported declared param's data type: %s", PLParserUtil.getFullText(variable_declarationContext)));
                }
                PlSqlParser.Native_datatype_elementContext native_datatype_elementContext = variable_declarationContext.type_spec().datatype().native_datatype_element();
                Tuple2<Integer, Integer> precisionAndScale = plDataTypePrecisionVisitor.visit(variable_declarationContext.type_spec().datatype());
                PlType plType = getTargetType(native_datatype_elementContext.getText(), precisionAndScale._1(), precisionAndScale._2());
                declaredParams.put(variable_declarationContext.identifier().getText(), plType);

                PlSqlParser.Default_value_partContext default_value = variable_declarationContext.default_value_part();
                if (default_value != null) {
                    defaultValues.put(variable_declarationContext.identifier().getText(), default_value.getChild(1).getText());
                }
            }
        }
        if (!declaredParams.isEmpty()) {
            return generateBaseOp(declaredParams, defaultValues);
        }
        return null;
    }

    private LogicalOperation generateBaseOp(Map<String, PlType> declareParams, Map<String, String> defaultValues) {
        List<Argument> arguments = new ArrayList<>();
        declareParams.forEach((param, type) -> {
            Argument argument = new Argument(param, type);
            String defaultVal = defaultValues.get(param);
            if (defaultVal != null) {
                argument.setDefaultVal(defaultVal);
            } else {
                argument.setDefaultVal("null");
            }
            arguments.add(argument);
        });

        LogicalDeclareParams declareParamsModel = new LogicalDeclareParams();
        declareParamsModel.setDeclareParams(arguments);
        return declareParamsModel;
    }
}
