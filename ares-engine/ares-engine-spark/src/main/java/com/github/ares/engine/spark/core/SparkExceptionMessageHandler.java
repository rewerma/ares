package com.github.ares.engine.spark.core;

import com.github.ares.com.google.inject.Singleton;
import com.github.ares.engine.core.ExceptionMessageHandler;
import org.apache.spark.SparkException;

import java.io.Serializable;


public class SparkExceptionMessageHandler extends ExceptionMessageHandler implements Serializable {
    private static final long serialVersionUID = -1L;

    @Override
    public String getMessage(Exception e) {
        if (e == null) {
            return null;
        }
        if (e.getCause() != null) {
            Exception resEx = fetchWithOutSparkException(e.getCause());
            if (resEx != null) {
                if (resEx.getMessage() != null) {
                    return resEx.getMessage();
                } else {
                    return resEx.toString();
                }
            }
        }
        return e.getMessage();
    }

    private Exception fetchWithOutSparkException(Throwable e) {
        if (e instanceof SparkException) {
            return fetchWithOutSparkException(e.getCause());
        } else {
            return (Exception) e;
        }
    }
}
