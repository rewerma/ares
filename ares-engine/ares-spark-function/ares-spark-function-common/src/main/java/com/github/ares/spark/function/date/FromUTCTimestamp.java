//package com.github.ares.spark.function.date;
//
//import com.github.ares.api.table.type.AresDataType;
//import com.github.ares.api.table.type.LocalTimeType;
//import com.github.ares.sql.function.SparkFuncInterface;
//import com.google.auto.service.AutoService;
//import org.apache.spark.unsafe.types.UTF8String;
//
//import java.time.LocalDateTime;
//import java.util.List;
//
//import static com.github.ares.common.utils.DateTimeUtils.localDateTimeToMicros;
//import static com.github.ares.common.utils.DateTimeUtils.microsToLocalDateTime;
//import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
//import static com.github.ares.sql.function.utils.Utils.toLocalDateTime;
//import static com.github.ares.sql.function.utils.Utils.toStr;
//
//@AutoService(SparkFuncInterface.class)
//public class FromUTCTimestamp implements SparkFuncInterface {
//    @Override
//    public String functionName() {
//        return "FROM_UTC_TIMESTAMP";
//    }
//
//    @Override
//    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
//        return LocalTimeType.LOCAL_DATE_TIME_TYPE;
//    }
//
//    @Override
//    public Object evaluate(List<Object> args) {
//        validateArgCount(functionName(), 2, args.size());
//        LocalDateTime arg = toLocalDateTime(args.get(0));
//        String arg1 = toStr(args.get(1));
//
//        if (arg == null || arg1 == null) {
//            return null;
//        }
//        long argInMicros = localDateTimeToMicros(arg);
//        org.apache.spark.sql.catalyst.expressions.FromUTCTimestamp fromUtcTimestamp =
//                new org.apache.spark.sql.catalyst.expressions.FromUTCTimestamp(null, null);
//        long res = (Long) fromUtcTimestamp.nullSafeEval(argInMicros, UTF8String.fromString(arg1));
//        return microsToLocalDateTime(res);
//    }
//}
