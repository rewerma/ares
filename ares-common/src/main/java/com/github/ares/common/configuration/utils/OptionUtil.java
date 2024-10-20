package com.github.ares.common.configuration.utils;

import com.github.ares.com.fasterxml.jackson.core.type.TypeReference;
import com.github.ares.common.configuration.Option;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OptionUtil {

    private OptionUtil() {
    }

    public static String getOptionKeys(List<Option<?>> options) {
        StringBuilder builder = new StringBuilder();
        boolean flag = false;
        for (Option<?> option : options) {
            if (flag) {
                builder.append(", ");
            }
            builder.append("'").append(option.key()).append("'");
            flag = true;
        }
        return builder.toString();
    }

    public static String getOptionKeys(
            List<Option<?>> options, List<RequiredOption.BundledRequiredOptions> bundledOptions) {
        List<List<Option<?>>> optionList = new ArrayList<>();
        for (Option<?> option : options) {
            optionList.add(Collections.singletonList(option));
        }
        for (RequiredOption.BundledRequiredOptions bundledOption : bundledOptions) {
            optionList.add(bundledOption.getRequiredOption());
        }
        boolean flag = false;
        StringBuilder builder = new StringBuilder();
        for (List<Option<?>> optionSet : optionList) {
            if (flag) {
                builder.append(", ");
            }
            builder.append("[").append(getOptionKeys(optionSet)).append("]");
            flag = true;
        }
        return builder.toString();
    }

    public static List<Option<?>> getOptions(Class<?> clazz)
            throws InstantiationException, IllegalAccessException {
        Field[] fields = clazz.getDeclaredFields();
        List<Option<?>> options = new ArrayList<>();
        Object object = clazz.newInstance();
        for (Field field : fields) {
            field.setAccessible(true);
            OptionMark option = field.getAnnotation(OptionMark.class);
            if (option != null) {
                options.add(
                        new Option<>(
                                StringUtils.isEmpty(option.name())
                                        ? formatUnderScoreCase(field.getName())
                                        : option.name(),
                                new TypeReference<Object>() {
                                    @Override
                                    public Type getType() {
                                        return field.getType();
                                    }
                                },
                                field.get(object))
                                .withDescription(option.description()));
            }
        }
        return options;
    }

    private static String formatUnderScoreCase(String camel) {
        StringBuilder underScore =
                new StringBuilder(String.valueOf(Character.toLowerCase(camel.charAt(0))));
        for (int i = 1; i < camel.length(); i++) {
            char c = camel.charAt(i);
            underScore.append(Character.isLowerCase(c) ? c : "_" + Character.toLowerCase(c));
        }
        return underScore.toString();
    }
}
