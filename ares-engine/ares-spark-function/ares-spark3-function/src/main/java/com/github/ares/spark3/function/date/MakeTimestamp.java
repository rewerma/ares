package com.github.ares.spark3.function.date;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.common.utils.DateTimeUtils;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Option;
import scala.math.BigDecimal;

import java.time.ZoneId;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;


@AutoService(SparkFuncInterface.class)
public class MakeTimestamp implements SparkFuncInterface {

    @Override
    public String functionName() {
        return "MAKE_TIMESTAMP";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return LocalTimeType.LOCAL_DATE_TIME_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), new int[]{6, 7}, args.size());
        Number arg1 = toNumber(args.get(0));
        Number arg2 = toNumber(args.get(1));
        Number arg3 = toNumber(args.get(2));
        Number arg4 = toNumber(args.get(3));
        Number arg5 = toNumber(args.get(4));
        Number arg6 = toNumber(args.get(5));
        if (arg1 == null || arg2 == null || arg3 == null
                || arg4 == null || arg5 == null || arg6 == null) {
            return null;
        }
        String zone = ZoneId.systemDefault().getId();
        if (args.size() == 7) {
            zone = (String) args.get(6);
        }
        Option<Object> option = Option.apply(UTF8String.fromString(zone));
        Decimal decimal = new Decimal();
        decimal.set(new BigDecimal(java.math.BigDecimal.valueOf(arg6.doubleValue())), 20, 6);
        org.apache.spark.sql.catalyst.expressions.MakeTimestamp makeTimestamp =
                new org.apache.spark.sql.catalyst.expressions.MakeTimestamp(
                        null, null, null, null, null, null);
        Object res = makeTimestamp.nullSafeEval(arg1.intValue(), arg2.intValue(), arg3.intValue(), arg4.intValue(), arg5.intValue(), decimal, option);
        return DateTimeUtils.microsToLocalDateTime((Long) res);
    }
}
