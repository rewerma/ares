package com.github.ares.engine.function;

import static com.github.ares.common.utils.StringUtils.println;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.UdfInterface;
import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.List;

@AutoService(UdfInterface.class)
public class PutLineFunction implements UdfInterface {

    /**
     * Prefix on each printed line so a caller can lift driver stdout into a task result.
     * Keep in sync with {@code com.peacock.common.constants.Constants#PUT_LINE_LOG_MARKER}.
     */
    public static final String OUTPUT_PREFIX = "[PUT_LINE] ";

    @Override
    public String functionName() {
        return "put_line";
    }

    @Override
    public AresDataType<?> resultType() {
        return BasicType.STRING_TYPE;
    }

    @Override
    public List<AresDataType<?>> argTypes() {
        return Collections.singletonList(BasicType.STRING_TYPE);
    }

    @Override
    public Object evaluate(List<Object> args) {
        if (args.size() != 1) {
            throw new IllegalArgumentException("put_line function expects one argument");
        }
        Object value = args.get(0);
        String text = value == null ? "null" : String.valueOf(value);
        // Keep a trailing empty piece so a value that ends with a newline still prints a blank line.
        String[] lines = text.split("\\R", -1);
        if (lines.length == 0) {
            println(OUTPUT_PREFIX);
        } else {
            for (String line : lines) {
                println(OUTPUT_PREFIX + line);
            }
        }
        return value;
    }
}
