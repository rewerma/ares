package com.github.ares.spark.function.system;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.utils.DateTimeUtils;
import com.github.ares.common.utils.Tuple2;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.Murmur3Hash;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.unsafe.types.UTF8String;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@AutoService(SparkFuncInterface.class)
public class Hash implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "HASH";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.INT_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        if (args.isEmpty()) {
            throw new AresException(
                    String.format(
                            "The `hash` requires > 0 parameters but the actual number is %d", args.size()));
        }
        int hash = 42;
        int i = 0;
        int len = args.size();
        while (i < len) {
            Murmur3Hash hashExpression = new Murmur3Hash(null);
            Tuple2<Object, DataType> arg = checkArg(args.get(i));
            hash = hashExpression.computeHash(arg._1(), arg._2(), hash);
            i += 1;
        }
        return hash;
    }

    private static Tuple2<Object, DataType> checkArg(Object arg) {
        if (arg instanceof Byte) {
            return Tuple2.of(arg, DataTypes.ByteType);
        }
        if (arg instanceof Short) {
            return Tuple2.of(arg, DataTypes.ShortType);
        }
        if (arg instanceof Integer) {
            return Tuple2.of(arg, DataTypes.IntegerType);
        }
        if (arg instanceof Long) {
            return Tuple2.of(arg, DataTypes.LongType);
        }
        if (arg instanceof Double) {
            return Tuple2.of(arg, DataTypes.DoubleType);
        }
        if (arg instanceof Float) {
            return Tuple2.of(arg, DataTypes.FloatType);
        }
        if (arg instanceof Decimal) {
            Decimal decimal = (Decimal) arg;
            return Tuple2.of(arg, new DecimalType(decimal.precision(), decimal.scale()));
        }
        if (arg instanceof String) {
            return Tuple2.of(UTF8String.fromString(arg.toString()), DataTypes.StringType);
        }
        if (arg instanceof byte[]) {
            return Tuple2.of(arg, DataTypes.BinaryType);
        }
        if (arg instanceof Boolean) {
            return Tuple2.of(arg, DataTypes.BooleanType);
        }
        if (arg instanceof LocalDate) {
            return Tuple2.of(((LocalDate) arg).toEpochDay(), DataTypes.DateType);
        }
        if (arg instanceof LocalDateTime) {
            return Tuple2.of(DateTimeUtils.localDateTimeToMicros((LocalDateTime) arg), DataTypes.DateType);
        }
        throw new AresException(String.format("Unsupported data type: %s", arg.getClass().getSimpleName()));
    }

}
