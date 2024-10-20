package com.github.ares.spark.function.date;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.spark.function.utils.StringTypeExpression;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.DateFormatClass;
import scala.Option;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static com.github.ares.common.utils.DateTimeUtils.localDateTimeToMicros;
import static com.github.ares.common.utils.DateTimeUtils.stringToLocalDateTime;
import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class DateFormat implements SparkFuncInterface {

    @Override
    public String functionName() {
        return "DATE_FORMAT";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return null;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        Object arg0 = args.get(0);
        if (arg0 == null) {
            return null;
        }
        LocalDateTime datetime = null;
        if (arg0 instanceof String) {
            datetime = stringToLocalDateTime((String) arg0);
        }
        if (arg0 instanceof LocalDateTime) {
            datetime = (LocalDateTime) arg0;
        } else if (arg0 instanceof LocalDate) {
            datetime = ((LocalDate) arg0).atStartOfDay();
        }
        if (datetime == null) {
            return null;
        }
        String format = toStr(args.get(1));
        long micros = localDateTimeToMicros(datetime);
        Option<String> option = Option.apply(ZoneId.systemDefault().getId());
        DateFormatClass dateFormatClass = new DateFormatClass(null, new StringTypeExpression(), option);
        return dateFormatClass.nullSafeEval(micros, format).toString();
    }
}
