package com.github.ares.sql.function.utils;

import com.github.ares.common.exceptions.AresException;

import java.util.StringJoiner;

public class FunctionArgumentValid {
    public static void validateArgCount(String functionName, int argumentCount, int inputCount) {
        if (inputCount == argumentCount) {
            return;
        }
        throw new AresException(
                String.format(
                        "Invalid number of arguments for function %s. Expected: %d; Found: %d",
                        functionName, argumentCount, inputCount));
    }

    public static void validateArgCount(String functionName, int[] argumentCounts, int inputCount) {
        StringJoiner joiner = new StringJoiner(" and ");
        for (Integer argumentCount : argumentCounts) {
            if (inputCount == argumentCount) {
                return;
            }
            joiner.add(String.valueOf(argumentCount));
        }
        throw new AresException(
                String.format(
                        "Invalid number of arguments for function %s. Expected: one of %s; Found: %d",
                        functionName, joiner, inputCount));
    }
}