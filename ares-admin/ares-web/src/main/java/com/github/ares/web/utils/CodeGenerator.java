package com.github.ares.web.utils;

import com.aventrix.jnanoid.jnanoid.NanoIdUtils;

public class CodeGenerator {
    public static String generateCode() {
        return NanoIdUtils.randomNanoId(NanoIdUtils.DEFAULT_NUMBER_GENERATOR, NanoIdUtils.DEFAULT_ALPHABET, 8);
    }
}
