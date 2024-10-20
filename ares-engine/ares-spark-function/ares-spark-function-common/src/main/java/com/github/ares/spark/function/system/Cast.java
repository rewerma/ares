package com.github.ares.spark.function.system;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.spark.function.date.ToDate;
import com.github.ares.spark.function.date.ToTimestamp;
import com.github.ares.spark.function.string.Hex;
import com.github.ares.spark.function.string.Unhex;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class Cast implements SparkFuncInterface {
    private final ToDate toDate = new ToDate();
    private final ToTimestamp toTimestamp = new ToTimestamp();
    private final Hex hex = new Hex();
    private final Unhex unhex = new Unhex();

    @Override
    public String functionName() {
        return "CAST";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.ANY_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), new int[]{2, 3, 4}, args.size());
        Object arg = args.get(0);
        if (arg == null) {
            return null;
        }
        String type = toStr(args.get(1));
        switch (type.toUpperCase()) {
            case "INT":
                return new BigDecimal(arg.toString()).intValue();
            case "SMALLINT":
                return new BigDecimal(arg.toString()).shortValue();
            case "BYTE":
                return new BigDecimal(arg.toString()).byteValue();
            case "BIGINT":
                return new BigDecimal(arg.toString()).longValue();
            case "BOOLEAN":
                if (arg instanceof Boolean) {
                    return arg;
                } else if (arg instanceof Number) {
                    return ((Number) arg).intValue() != 0;
                }
            case "DOUBLE":
                return new BigDecimal(arg.toString()).doubleValue();
            case "FLOAT":
                return new BigDecimal(arg.toString()).floatValue();
            case "NUMBER":
            case "DECIMAL":
                BigDecimal bigDecimal = new BigDecimal(arg.toString());
                Integer scale = (Integer) args.get(3);
                return bigDecimal.setScale(scale, RoundingMode.HALF_UP);
            case "STRING":
            case "VARCHAR":
                if (arg instanceof byte[]) {
                    return new String((byte[]) arg, StandardCharsets.UTF_8);
                } else {
                    return arg.toString();
                }
            case "DATE":
                if (arg instanceof LocalDateTime) {
                    return ((LocalDateTime) arg).toLocalDate();
                } else if (arg instanceof LocalDate) {
                    return arg;
                } else if (arg instanceof String) {
                    return toDate.evaluate(Collections.singletonList(arg));
                } else {
                    throw new AresException("cannot cast \"" + arg.getClass().getSimpleName() + "\" to \"DATE\" type");
                }
            case "TIMESTAMP":
                return toTimestamp.evaluate(Collections.singletonList(arg));
            case "BINARY":
            case "BLOB":
                Object hexValue = hex.evaluate(Collections.singletonList(arg));
                return unhex.evaluate(Collections.singletonList(hexValue));
            default:
                throw new AresException("Unsupported data type: " + type);
        }
    }
}
