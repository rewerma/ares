package com.github.ares.parser.visitor;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.common.utils.Tuple2;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.model.Argument;
import com.github.ares.parser.plan.LogicalCreateProcedure;
import com.github.ares.parser.plan.LogicalDeclareParams;
import com.github.ares.parser.plan.LogicalExceptionHandler;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.utils.PLParserUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PlCreateProcedureVisitor {
    private PlVisitorManager visitorManager;

    @Inject
    private PlDataTypePrecisionVisitor plDataTypePrecisionVisitor;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    public LogicalOperation visitCreateProcedure(PlSqlParser.Create_procedure_bodyContext createProcedureBody, List<LogicalOperation> baseBody) {
        String procedureName = createProcedureBody.procedure_name().identifier().getText();
        List<PlSqlParser.ParameterContext> params = createProcedureBody.parameter();
        Map<String, PlType> inParams = new LinkedHashMap<>();
        Map<String, PlType> outParams = new LinkedHashMap<>();
        List<Integer> outParamsIdx = new ArrayList<>();
        if (params != null) {
            int idx = 0;
            for (PlSqlParser.ParameterContext parameterContext : params) {
                PlSqlParser.Parameter_nameContext parameterNameContext = parameterContext.parameter_name();
                PlSqlParser.Type_specContext childTypeSpecContext = parameterContext.type_spec();
                if (childTypeSpecContext.datatype() == null) {
                    throw new ParseException(String.format("Unsupported param's data type: %s", PLParserUtil.getFullText(childTypeSpecContext)));
                }
                PlSqlParser.Native_datatype_elementContext nativeDatatypeElementContext = childTypeSpecContext.datatype().native_datatype_element();
                Tuple2<Integer, Integer> precisionAndScale = plDataTypePrecisionVisitor.visit(childTypeSpecContext.datatype());
                if (!parameterContext.IN().isEmpty() && "IN".equalsIgnoreCase(parameterContext.IN().get(0).getText())) {
                    inParams.put(parameterNameContext.getText(),
                            PLParserUtil.getTargetType(nativeDatatypeElementContext.getText(), precisionAndScale._1(), precisionAndScale._2()));
                } else if (!parameterContext.OUT().isEmpty() && "OUT".equalsIgnoreCase(parameterContext.OUT().get(0).getText())) {
                    outParams.put(parameterNameContext.getText(),
                            PLParserUtil.getTargetType(nativeDatatypeElementContext.getText(), precisionAndScale._1(), precisionAndScale._2()));
                    outParamsIdx.add(idx);
                }
                idx++;
            }
        }

        List<LogicalOperation> procOperation = new ArrayList<>();

        Map<String, PlType> declaredParams = new LinkedHashMap<>();
        LogicalOperation declareOperation = visitorManager.getDeclareParamsVisitor()
                .visitDeclareParams(createProcedureBody.seq_of_declare_specs(), declaredParams);

        PlSqlParser.Seq_of_statementsContext seqOfStatementsContext = createProcedureBody.body().seq_of_statements();
        if (seqOfStatementsContext != null) {
            procOperation.addAll(visitorManager.getBodyVisitor()
                    .visitBodyStatements(seqOfStatementsContext, inParams, outParams, declaredParams, baseBody, null));
        }


        LogicalCreateProcedure createProcedure = new LogicalCreateProcedure();
        createProcedure.setProcedureName(procedureName);
        createProcedure.setProcedureBody(procOperation);
        List<Argument> inArgList = new ArrayList<>();
        inParams.forEach((k, v) -> {
            Argument argument = new Argument(k, v);
            inArgList.add(argument);
        });
        createProcedure.setInArgs(inArgList);
        List<Argument> outArgList = new ArrayList<>();
        outParams.forEach((k, v) -> {
            Argument argument = new Argument(k, v);
            outArgList.add(argument);
        });
        createProcedure.setOutArgs(outArgList);
        createProcedure.setOutArgsIndex(outParamsIdx);
        createProcedure.setDeclareParams((LogicalDeclareParams) declareOperation);

        if (createProcedureBody.body().exception_handler() != null &&
                !createProcedureBody.body().exception_handler().isEmpty()) {
            LogicalExceptionHandler exHandler = visitorManager.getExceptionHandlerVisitor()
                    .visitExceptionHandler(createProcedureBody.body().exception_handler().get(0), inParams, outParams, declaredParams, false);
            if (exHandler != null) {
                createProcedure.setExHandler(exHandler);
            }
        }
        return createProcedure;
    }
}
