package com.github.ares.parser.model;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalOperation;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@EqualsAndHashCode(callSuper = true)
@Getter
@Setter
public abstract class CreateMethod extends LogicalOperation implements Serializable {
    protected List<Argument> inArgs = new ArrayList<>();

    public CreateMethod(OperationType plainType) {
        super(plainType);
    }
}
