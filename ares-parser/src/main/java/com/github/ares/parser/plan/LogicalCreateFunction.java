package com.github.ares.parser.plan;

import static com.github.ares.parser.utils.PLParserUtil.getOriginalType;

import com.github.ares.common.engine.PlType;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.model.CreateMethod;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LogicalCreateFunction extends CreateMethod implements Serializable {
    private static final long serialVersionUID = -1L;

    private String functionName;
    private LogicalDeclareParams declareParams;
    private List<LogicalOperation> functionBody = new ArrayList<>();
    private PlType returnType;

    private LogicalExceptionHandler exHandler;

    public LogicalCreateFunction() {
        super(OperationType.CREATE_FUNCTION);
    }

    public String getArgsString() {
        StringJoiner sb = new StringJoiner(", ");
        inArgs.forEach(arg -> sb.add(arg.getName() + " " + getOriginalType(arg.getPlType())));
        return sb.toString();
    }
}
