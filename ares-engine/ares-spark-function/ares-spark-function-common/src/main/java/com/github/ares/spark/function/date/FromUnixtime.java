//package com.github.ares.spark.function.date;
//
//import com.github.ares.api.table.type.AresDataType;
//import com.github.ares.api.table.type.LocalTimeType;
//import com.github.ares.common.utils.DateTimeUtils;
//import com.github.ares.sql.function.SparkFuncInterface;
//import com.google.auto.service.AutoService;
//import org.apache.spark.unsafe.types.UTF8String;
//
//import java.time.LocalDateTime;
//import java.time.ZoneOffset;
//import java.time.ZonedDateTime;
//import java.util.Arrays;
//import java.util.List;
//
//import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
//import static com.github.ares.sql.function.utils.Utils.toNumber;
//
//@AutoService(SparkFuncInterface.class)
//public class FromUnixtime implements SparkFuncInterface {
//
//    @Override
//    public String functionName() {
//        return "FROM_UNIXTIME";
//    }
//
//    @Override
//    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
//        return LocalTimeType.LOCAL_DATE_TIME_TYPE;
//    }
//
//    @Override
//    public Object evaluate(List<Object> args) {
//        validateArgCount(functionName(), new int[]{1, 2}, args.size());
//        Number arg = toNumber(args.get(0));
//        if (arg == null) {
//            return null;
//        }
//        LocalDateTime dateTime = DateTimeUtils.getLocalDateTime(arg.intValue() * 1000000L, ZoneOffset.UTC);
//        ZonedDateTime zonedDateTime = dateTime.atZone(ZoneOffset.UTC).withZoneSameInstant(ZoneOffset.systemDefault());
//        dateTime = zonedDateTime.toLocalDateTime();
//
//        String format;
//        if (args.size() > 1) {
//            format = (String) args.get(1);
//        } else {
//            format = "yyyy-MM-dd HH:mm:ss";
//        }
//        DateFormat dateFormat = new DateFormat();
//        return dateFormat.evaluate(Arrays.asList(dateTime, format)).toString();
//    }
//}
