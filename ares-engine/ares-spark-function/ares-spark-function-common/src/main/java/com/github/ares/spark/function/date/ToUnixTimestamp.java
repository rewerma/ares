//package com.github.ares.spark.function.date;
//
//import com.github.ares.api.table.type.AresDataType;
//import com.github.ares.api.table.type.BasicType;
//import com.github.ares.common.exceptions.AresException;
//import com.github.ares.common.utils.DateTimeUtils;
//import com.github.ares.sql.function.SparkFuncInterface;
//import com.google.auto.service.AutoService;
//
//import java.time.LocalDate;
//import java.time.LocalDateTime;
//import java.time.ZoneOffset;
//import java.time.ZonedDateTime;
//import java.time.format.DateTimeFormatter;
//import java.time.format.DateTimeParseException;
//import java.util.List;
//
//import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
//import static com.github.ares.sql.function.utils.Utils.toLocalDateTime;
//import static com.github.ares.sql.function.utils.Utils.toStr;
//
//@AutoService(SparkFuncInterface.class)
//public class ToUnixTimestamp implements SparkFuncInterface {
//
//    @Override
//    public String functionName() {
//        return "TO_UNIX_TIMESTAMP";
//    }
//
//    @Override
//    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
//        return BasicType.LONG_TYPE;
//    }
//
//    @Override
//    public Object evaluate(List<Object> args) {
//        validateArgCount(functionName(), new int[]{1, 2}, args.size());
//        Object arg = args.get(0);
//        if (arg == null) {
//            return null;
//        }
//        LocalDateTime dateTime = null;
//        if (arg instanceof LocalDateTime) {
//            dateTime = (LocalDateTime) arg;
//        } else if (arg instanceof LocalDate) {
//            dateTime = ((LocalDate) arg).atStartOfDay();
//        } else if (arg instanceof String) {
//            String format = null;
//            if (args.size() == 2) {
//                format = toStr(args.get(1));
//            }
//            if (format != null) {
//                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
//                try {
//                    dateTime = LocalDateTime.parse(toStr(args.get(0)), formatter);
//                } catch (DateTimeParseException e) {
//                    dateTime = LocalDate.parse(toStr(args.get(0)), formatter).atStartOfDay();
//                }
//            } else {
//                dateTime = toLocalDateTime(arg);
//            }
//        } else {
//            throw new AresException("Cannot resolve \"to_unix_timestamp(" + arg + ")\" due to data type mismatch: " +
//                    "Parameter 1 requires the (\"STRING\" or \"DATE\" or \"TIMESTAMP\") type, however \"" + arg + "\" has the type \"" + arg.getClass().getSimpleName() + "\".");
//        }
//
//        ZonedDateTime zonedDateTime = dateTime.atZone(ZoneOffset.systemDefault()).withZoneSameInstant(ZoneOffset.UTC);
//        dateTime = zonedDateTime.toLocalDateTime();
//        long res = DateTimeUtils.localDateTimeToMicros(dateTime) / 1000000L;
////        FromUnixtime fromUnixtime = new FromUnixtime();
////        Object res2 = fromUnixtime.evaluate(Arrays.asList(1460098800L));
//        return res;
//    }
//}
