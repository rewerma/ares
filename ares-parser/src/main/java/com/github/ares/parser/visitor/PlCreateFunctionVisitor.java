package com.github.ares.parser.visitor;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.common.utils.Tuple2;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.model.Argument;
import com.github.ares.parser.plan.LogicalCreateFunction;
import com.github.ares.parser.plan.LogicalDeclareParams;
import com.github.ares.parser.plan.LogicalExceptionHandler;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.utils.PLParserUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.github.ares.parser.utils.PLParserUtil.getTargetType;

public class PlCreateFunctionVisitor {
    private PlVisitorManager visitorManager;

    @Inject
    PlDataTypePrecisionVisitor plDataTypePrecisionVisitor;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    public LogicalOperation visitCreateFunction(PlSqlParser.Create_function_bodyContext createFunctionBodyContext, List<LogicalOperation> baseBody) {
        PlSqlParser.Function_bodyContext function_bodyContext = createFunctionBodyContext.function_body();
        String functionName = function_bodyContext.identifier().getText();
        if (createFunctionBodyContext.function_body().type_spec() == null) {
            throw new ParseException(String.format("Return type of function '%s' must be specified", functionName));
        }
        Tuple2<Integer, Integer> precisionAndScale = plDataTypePrecisionVisitor.visit(createFunctionBodyContext.function_body().type_spec().datatype());
        PlType returnType = getTargetType(createFunctionBodyContext.function_body().type_spec().datatype().native_datatype_element().getText(), precisionAndScale._1(), precisionAndScale._2());
        List<PlSqlParser.ParameterContext> params = function_bodyContext.parameter();
        Map<String, PlType> inParams = new LinkedHashMap<>();
        if (params != null && !params.isEmpty()) {
            for (PlSqlParser.ParameterContext parameterContext : params) {
                PlSqlParser.Parameter_nameContext parameterNameContext = parameterContext.parameter_name();
                PlSqlParser.Type_specContext childType_specContext = parameterContext.type_spec();
                if (childType_specContext.datatype() == null) {
                    throw new ParseException(String.format("Unsupported param's data type: %s", PLParserUtil.getFullText(childType_specContext)));
                }
                PlSqlParser.Native_datatype_elementContext native_datatype_elementContext = childType_specContext.datatype().native_datatype_element();
                Tuple2<Integer, Integer> precisionAndScaleParam = plDataTypePrecisionVisitor.visit(parameterContext.type_spec().datatype());
                inParams.put(parameterNameContext.getText(), PLParserUtil.getTargetType(native_datatype_elementContext.getText(), precisionAndScaleParam._1(), precisionAndScaleParam._2()));
            }
        } else {
            throw new ParseException(String.format("The function '%s' must have at least one parameter", functionName));
        }

        List<LogicalOperation> procOperation = new ArrayList<>();
        Map<String, PlType> declaredParams = new LinkedHashMap<>();
        LogicalOperation declareOperation = visitorManager.getDeclareParamsVisitor()
                .visitDeclareParams(function_bodyContext.seq_of_declare_specs(), declaredParams);
        PlSqlParser.Seq_of_statementsContext seq_of_statementsContext = function_bodyContext.body().seq_of_statements();
        if (seq_of_statementsContext != null) {
            declaredParams.put("_return_type_", returnType);
            procOperation.addAll(visitorManager.getFunctionBodyVisitor()
                    .visitBodyStatements(seq_of_statementsContext.statement(), inParams, new LinkedHashMap<>(), declaredParams, baseBody, null));
        }

        LogicalCreateFunction createFunction = new LogicalCreateFunction();
        createFunction.setFunctionName(functionName);
        createFunction.setFunctionBody(procOperation);
        createFunction.setReturnType(returnType);
        createFunction.setDeclareParams((LogicalDeclareParams) declareOperation);

        List<Argument> inArgList = new ArrayList<>();
        inParams.forEach((k, v) -> {
            Argument argument = new Argument(k, v);
            inArgList.add(argument);
        });
        createFunction.setInArgs(inArgList);

        if (createFunctionBodyContext.function_body().body() != null &&
                createFunctionBodyContext.function_body().body().exception_handler() != null &&
                !createFunctionBodyContext.function_body().body().exception_handler().isEmpty()) {
            LogicalExceptionHandler exHandler = visitorManager.getExceptionHandlerVisitor()
                    .visitExceptionHandler(createFunctionBodyContext.function_body().body().exception_handler().get(0),
                            inParams, new LinkedHashMap<>(), declaredParams, true);
            if (exHandler != null) {
                createFunction.setExHandler(exHandler);
            }
        }
        return createFunction;
    }
}
