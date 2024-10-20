package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.model.Argument;
import com.github.ares.parser.model.CreateMethod;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.github.ares.parser.utils.PLParserUtil.getOriginalType;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class LogicalCreateProcedure extends CreateMethod implements Serializable {
    private static final long serialVersionUID = 1L;

    private String procedureName;

    private LogicalDeclareParams declareParams;

    private List<Argument> outArgs;

    private List<Integer> outArgsIndex;

    private List<LogicalOperation> procedureBody = new ArrayList<>();

    private LogicalExceptionHandler exHandler;

    public LogicalCreateProcedure() {
        super(OperationType.CREATE_PROCEDURE);
    }

    public String getArgsString() {
        int paramCount = inArgs.size() + outArgs.size();
        List<Integer> inArgsIndex = new ArrayList<>();
        for (int i = 0; i < paramCount; i++) {
            if (outArgsIndex.contains(i)) {
                continue;
            }
            inArgsIndex.add(i);
        }
        List<String> args = new ArrayList<>(paramCount);
        for (int i = 0; i < paramCount; i++) {
            args.add(null);
        }
        for (int i = 0; i < inArgs.size(); i++) {
            Integer index = inArgsIndex.get(i);
            Argument arg = inArgs.get(i);
            String argString = arg.getName() + " IN " + getOriginalType(arg.getPlType());
            args.set(index, argString);
        }
        for (int i = 0; i < outArgs.size(); i++) {
            Integer index = outArgsIndex.get(i);
            Argument arg = outArgs.get(i);
            String argString = arg.getName() + " OUT " + getOriginalType(arg.getPlType());
            args.set(index, argString);
        }

        return String.join(", ", args);
    }
}
