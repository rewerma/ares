package com.github.ares.engine.core;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.com.google.inject.Singleton;
import com.github.ares.parser.config.PlProperties;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class TraceLogger implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final org.slf4j.Logger log = LoggerFactory.getLogger("[PL-Trace]: ");

    public PlProperties plProperties;

    public void init(PlProperties plProperties) {
        this.plProperties = plProperties;
    }

    public void info(String message) {
        if (isTraceEnabled()) {
            log.info(message);
        }
    }

    public void info(String format, Object arg) {
        if (isTraceEnabled()) {
            arg = cleanParams(arg);
            log.info(format, arg);
        }
    }

    public void info(String format, Object arg1, Object arg2) {
        if (isTraceEnabled()) {
            arg1 = cleanParams(arg1);
            arg2 = cleanParams(arg2);
            log.info(format, arg1, arg2);
        }
    }

    public void info(String format, Object... arguments) {
        if (isTraceEnabled()) {
            for (int i = 0; i < arguments.length; i++) {
                Object arg = cleanParams(arguments[i]);
                arguments[i] = arg;
            }
            log.info(format, arguments);
        }
    }

    private boolean isTraceEnabled() {
        Object traceEnabled = plProperties.getProperties().get("ares.pl.trace.enabled");
        return traceEnabled == null || !traceEnabled.equals("false");
    }

    private Object cleanParams(Object arg) {
        if (arg instanceof String) {
            return ((String) arg).replaceAll("\"\\$\\{([^}]+)\\}\"", "$1");
        } else {
            return arg;
        }
    }
}
