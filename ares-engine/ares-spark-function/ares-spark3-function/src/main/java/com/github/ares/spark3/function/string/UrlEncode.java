package com.github.ares.spark3.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class UrlEncode implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "URL_ENCODE";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 1, args.size());
        String src = toStr(args.get(0));
        try {
           return URLEncoder.encode(src, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new AresException("Unsupported encoding");
        }
    }
}
