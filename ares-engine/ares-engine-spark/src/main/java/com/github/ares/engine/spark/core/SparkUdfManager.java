package com.github.ares.engine.spark.core;

import com.github.ares.common.exceptions.AresException;
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.engine.core.UdfManager;
import com.github.ares.engine.spark.utils.FunctionHelper;
import com.github.ares.sql.function.DynamicFunction;
import org.apache.spark.sql.SparkSession;
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

import static com.github.ares.engine.spark.utils.FunctionHelper.convertType;

public class SparkUdfManager extends UdfManager {
    private static final long serialVersionUID = -1L;

    private SparkExecutorManager sparkExecutorManager;

    public void init(ExecutorManager executorManager) {
        this.sparkExecutorManager = (SparkExecutorManager) executorManager;
        super.init(executorManager);
    }

    public void registerUdf(DynamicFunction dynamicFunction) {
        if (dynamicFunction.getResultType() == null) {
            return;
        }
        SparkSession sparkSession = sparkExecutorManager.getSparkSessionManager().getSparkSession();
        Object udf = FunctionHelper.convert(dynamicFunction);
        int argCount = dynamicFunction.getArgTypes().size();
        switch (argCount) {
            case 0:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF0<?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 1:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF1<?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 2:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF2<?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 3:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF3<?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 4:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF4<?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 5:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF5<?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 6:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF6<?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 7:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF7<?, ?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 8:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF8<?, ?, ?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 9:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF9<?, ?, ?, ?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 10:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF10<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 11:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF11<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 12:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF12<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 13:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF13<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 14:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF14<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 15:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF15<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 16:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF16<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 17:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF17<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 18:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF18<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 19:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF19<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 20:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF20<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 21:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF21<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            case 22:
                sparkSession.udf().register(dynamicFunction.getFunctionName(), (UDF22<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?>) udf,
                        convertType(dynamicFunction.getResultType()));
                break;
            default:
                throw new AresException("Unsupported number of arguments: " + argCount);
        }
    }

}
