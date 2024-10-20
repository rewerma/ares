package com.github.ares.common.utils;

public final class StringUtils {
    public static String substring(String str, int start, int end) {
        end = Math.min(str.length(), end);
        return str.substring(start, end);
    }

    public static void println(Object obj) {
        System.out.println(obj);
    }
}
