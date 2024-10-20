package com.github.ares.engine.spark.utils;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.DynamicFunction;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF10;
import org.apache.spark.sql.api.java.UDF11;
import org.apache.spark.sql.api.java.UDF12;
import org.apache.spark.sql.api.java.UDF13;
import org.apache.spark.sql.api.java.UDF14;
import org.apache.spark.sql.api.java.UDF15;
import org.apache.spark.sql.api.java.UDF16;
import org.apache.spark.sql.api.java.UDF17;
import org.apache.spark.sql.api.java.UDF18;
import org.apache.spark.sql.api.java.UDF19;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF20;
import org.apache.spark.sql.api.java.UDF21;
import org.apache.spark.sql.api.java.UDF22;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.api.java.UDF6;
import org.apache.spark.sql.api.java.UDF7;
import org.apache.spark.sql.api.java.UDF8;
import org.apache.spark.sql.api.java.UDF9;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class FunctionHelper {
    public static DataType convertType(AresDataType<?> aresDataType) {
        if (aresDataType.equals(BasicType.STRING_TYPE)) {
            return DataTypes.StringType;
        }
        if (aresDataType.equals(BasicType.INT_TYPE)) {
            return DataTypes.IntegerType;
        }
        if (aresDataType.equals(BasicType.LONG_TYPE)) {
            return DataTypes.LongType;
        }
        if (aresDataType.equals(BasicType.BYTE_TYPE)) {
            return DataTypes.ByteType;
        }
        if (aresDataType.equals(BasicType.SHORT_TYPE)) {
            return DataTypes.ShortType;
        }
        if (aresDataType.equals(BasicType.BOOLEAN_TYPE)) {
            return DataTypes.BooleanType;
        }
        if (aresDataType.equals(BasicType.FLOAT_TYPE)) {
            return DataTypes.FloatType;
        }
        if (aresDataType.equals(BasicType.DOUBLE_TYPE)) {
            return DataTypes.DoubleType;
        }
        if (aresDataType.equals(LocalTimeType.LOCAL_DATE_TYPE)) {
            return DataTypes.DateType;
        }
        if (aresDataType.equals(LocalTimeType.LOCAL_DATE_TIME_TYPE)) {
            return DataTypes.TimestampType;
        }
        if (aresDataType.equals(ArrayType.BYTE_ARRAY_TYPE)) {
            return DataTypes.BinaryType;
        }
        if (aresDataType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) aresDataType;
            return DataTypes.createDecimalType(decimalType.getPrecision(), decimalType.getScale());
        }

        throw new RuntimeException("Unsupported data type: " + aresDataType);
    }

    public static Object convert(DynamicFunction dynamicFunction) {
        int argCount = dynamicFunction.getArgTypes().size();
        if (argCount == 0) {
            UDF0<?> udf0 = () -> dynamicFunction.evaluate(new ArrayList<>());
            return udf0;
        }
        if (argCount == 1) {
            UDF1<?, ?> udf1 = (UDF1<Object, Object>) v0 ->
                    dynamicFunction.evaluate(Collections.singletonList(v0));
            return udf1;
        }
        if (argCount == 2) {
            UDF2<?, ?, ?> udf2 = (UDF2<Object, Object, Object>) (v0, v1) ->
                    dynamicFunction.evaluate(Arrays.asList(v0, v1));
            return udf2;
        }
        if (argCount == 3) {
            UDF3<?, ?, ?, ?> udf3 = (UDF3<Object, Object, Object, Object>) (v0, v1, v2) ->
                    dynamicFunction.evaluate(Arrays.asList(v0, v1, v2));
            return udf3;
        }
        if (argCount == 4) {
            UDF4<?, ?, ?, ?, ?> udf4 = (UDF4<Object, Object, Object, Object, Object>) (v0, v1, v2, v3) ->
                    dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3));
            return udf4;
        }
        if (argCount == 5) {
            UDF5<?, ?, ?, ?, ?, ?> udf5 = (UDF5<Object, Object, Object, Object, Object, Object>) (v0, v1, v2, v3, v4) ->
                    dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4));
            return udf5;
        }
        if (argCount == 6) {
            UDF6<?, ?, ?, ?, ?, ?, ?> udf6 = (UDF6<Object, Object, Object, Object, Object, Object, Object>) (v0, v1, v2, v3, v4, v5) ->
                    dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5));
            return udf6;
        }
        if (argCount == 7) {
            UDF7<?, ?, ?, ?, ?, ?, ?, ?> udf7 = (UDF7<Object, Object, Object, Object, Object, Object, Object, Object>)
                    (v0, v1, v2, v3, v4, v5, v6) ->
                            dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5, v6));
            return udf7;
        }
        if (argCount == 8) {
            UDF8<?, ?, ?, ?, ?, ?, ?, ?, ?> udf8 = (UDF8<Object, Object, Object, Object, Object, Object,
                    Object, Object, Object>) (v0, v1, v2, v3, v4, v5, v6, v7) ->
                    dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5, v6, v7));
            return udf8;
        }
        if (argCount == 9) {
            UDF9<?, ?, ?, ?, ?, ?, ?, ?, ?, ?> udf9 = (UDF9<Object, Object, Object, Object, Object, Object, Object,
                    Object, Object, Object>) (v0, v1, v2, v3, v4, v5, v6, v7, v8) ->
                    dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5, v6, v7, v8));
            return udf9;
        }
        if (argCount == 10) {
            UDF10<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> udf10 = (UDF10<Object, Object, Object, Object, Object, Object,
                    Object, Object, Object, Object, Object>) (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9) ->
                    dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5, v6, v7, v8, v9));
            return udf10;
        }
        if (argCount == 11) {
            UDF11<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> udf11 = (UDF11<Object, Object, Object, Object, Object, Object,
                    Object, Object, Object, Object, Object, Object>) (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10) ->
                    dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10));
            return udf11;
        }
        if (argCount == 12) {
            UDF12<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> udf12 = (UDF12<Object, Object, Object, Object, Object, Object,
                    Object, Object, Object, Object, Object, Object, Object>) (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11) ->
                    dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11));
            return udf12;
        }
        if (argCount == 13) {
            UDF13<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> udf13 = (UDF13<Object, Object, Object, Object, Object, Object,
                    Object, Object, Object, Object, Object, Object, Object, Object>)
                    (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12) ->
                            dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12));
            return udf13;
        }
        if (argCount == 14) {
            UDF14<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> udf14 = (UDF14<Object, Object, Object, Object, Object,
                    Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>)
                    (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13) ->
                            dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13));
            return udf14;
        }
        if (argCount == 15) {
            UDF15<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> udf15 = (UDF15<Object, Object, Object, Object, Object,
                    Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>)
                    (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14) ->
                            dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14));
            return udf15;
        }
        if (argCount == 16) {
            UDF16<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> udf16 = (UDF16<Object, Object, Object, Object, Object,
                    Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>)
                    (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15) ->
                            dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15));
            return udf16;
        }
        if (argCount == 17) {
            UDF17<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> udf17 = (UDF17<Object, Object, Object, Object,
                    Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>)
                    (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16) ->
                            dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16));
            return udf17;
        }
        if (argCount == 18) {
            UDF18<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> udf18 = (UDF18<Object, Object, Object, Object,
                    Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>)
                    (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17) ->
                            dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17));
            return udf18;
        }
        if (argCount == 19) {
            UDF19<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> udf19 = (UDF19<Object, Object, Object,
                    Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>)
                    (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18) ->
                            dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18));
            return udf19;
        }
        if (argCount == 20) {
            UDF20<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> udf20 = (UDF20<Object, Object, Object,
                    Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>)
                    (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19) ->
                            dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19));
            return udf20;
        }
        if (argCount == 21) {
            UDF21<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> udf21 = (UDF21<Object, Object, Object,
                    Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>)
                    (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20) ->
                            dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20));
            return udf21;
        }
        if (argCount == 22) {
            UDF22<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> udf22 = (UDF22<Object, Object, Object,
                    Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object>)
                    (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20, v21) ->
                            dynamicFunction.evaluate(Arrays.asList(v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20, v21));
            return udf22;
        }

        throw new AresException("Unsupported number of arguments: " + argCount);
    }
}
