package com.github.ares.connctor.jdbc.internal.connection;

import com.github.ares.common.exceptions.AresException;
import com.github.ares.connctor.jdbc.config.JdbcConnectionConfig;
import com.google.common.base.CaseFormat;

import javax.sql.CommonDataSource;
import javax.sql.DataSource;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DataSourceUtils implements Serializable {
    private static final String GETTER_PREFIX = "get";

    private static final String SETTER_PREFIX = "set";

    public static CommonDataSource buildCommonDataSource(
             JdbcConnectionConfig jdbcConnectionConfig)
            throws InvocationTargetException, IllegalAccessException {
        CommonDataSource dataSource =
                (CommonDataSource) loadDataSource(jdbcConnectionConfig.getXaDataSourceClassName());
        setProperties(dataSource, buildDatabaseAccessConfig(jdbcConnectionConfig));
        return dataSource;
    }

    private static Map<String, Object> buildDatabaseAccessConfig(
            JdbcConnectionConfig jdbcConnectionConfig) {
        HashMap<String, Object> accessConfig = new HashMap<>();
        accessConfig.put("url", jdbcConnectionConfig.getUrl());
        if (jdbcConnectionConfig.getUsername().isPresent()) {
            accessConfig.put("user", jdbcConnectionConfig.getUsername().get());
        }
        if (jdbcConnectionConfig.getPassword().isPresent()) {
            accessConfig.put("password", jdbcConnectionConfig.getPassword().get());
        }
        accessConfig.putAll(jdbcConnectionConfig.getProperties());
        return accessConfig;
    }

    private static void setProperties(
            final CommonDataSource commonDataSource, final Map<String, Object> databaseAccessConfig)
            throws InvocationTargetException, IllegalAccessException {
        for (Map.Entry<String, Object> entry : databaseAccessConfig.entrySet()) {
            Optional<Method> method =
                    findSetterMethod(commonDataSource.getClass().getMethods(), entry.getKey());
            if (method.isPresent()) {
                method.get().invoke(commonDataSource, entry.getValue());
            }
        }
    }

    private static Method findGetterMethod(final DataSource dataSource, final String propertyName)
            throws NoSuchMethodException {
        String getterMethodName =
                GETTER_PREFIX + CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, propertyName);
        Method result = dataSource.getClass().getMethod(getterMethodName);
        result.setAccessible(true);
        return result;
    }

    private static Optional<Method> findSetterMethod(
            final Method[] methods, final String property) {
        String setterMethodName =
                SETTER_PREFIX + CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, property);
        Optional<Method> methodOptional =
                Arrays.stream(methods)
                        .filter(
                                each ->
                                        each.getName().equals(setterMethodName)
                                                && 1 == each.getParameterTypes().length)
                        .findFirst();
        if (!methodOptional.isPresent()) {
            methodOptional =
                    Arrays.stream(methods)
                            .filter(
                                    each ->
                                            each.getName().equalsIgnoreCase(setterMethodName)
                                                    && 1 == each.getParameterTypes().length)
                            .findFirst();
        }
        return methodOptional;
    }

    private static Object loadDataSource(final String xaDataSourceClassName) {
        Class<?> xaDataSourceClass;
        try {
            xaDataSourceClass =
                    Thread.currentThread().getContextClassLoader().loadClass(xaDataSourceClassName);
        } catch (final ClassNotFoundException ignored) {
            try {
                xaDataSourceClass = Class.forName(xaDataSourceClassName);
            } catch (final ClassNotFoundException ex) {
                throw new AresException(
                        "Failed to load [" + xaDataSourceClassName + "]",
                        ex);
            }
        }
        try {
            return xaDataSourceClass.getDeclaredConstructor().newInstance();
        } catch (final ReflectiveOperationException ex) {
            throw new AresException(
                    "Failed to instance [" + xaDataSourceClassName + "]",
                    ex);
        }
    }
}
