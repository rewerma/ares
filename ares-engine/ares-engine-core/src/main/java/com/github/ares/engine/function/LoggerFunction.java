package com.github.ares.engine.function;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.UdfInterface;
import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.github.ares.common.utils.StringUtils.println;

@AutoService(UdfInterface.class)
public class LoggerFunction implements UdfInterface {
    private static final Logger logger = LoggerFactory.getLogger("[PL-LOGGER]");

    @Override
    public String functionName() {
        return "logger";
    }

    @Override
    public AresDataType<?> resultType() {
        return BasicType.STRING_TYPE;
    }

    @Override
    public List<AresDataType<?>> argTypes() {
        return Arrays.asList(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
    }

    @Override
    public Object evaluate(List<Object> args) {
        if (args.size() != 2) {
            throw new IllegalArgumentException("logger function expects two argument");
        }
        String logLevel = (String) args.get(0);
        String logContent = (String) args.get(1);
        if ("INFO".equalsIgnoreCase(logLevel.trim())) {
            logger.info(logContent);
        } else if ("WARN".equalsIgnoreCase(logLevel.trim())) {
            logger.warn(logContent);
        } else if ("ERROR".equalsIgnoreCase(logLevel.trim())) {
            logger.error(logContent);
        } else if ("DEBUG".equalsIgnoreCase(logLevel.trim())) {
            logger.debug(logContent);
        }
        return logContent;
    }
}