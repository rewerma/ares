package com.github.ares.common.utils;

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class EncodingUtils {

    /**
     * try to parse charset by encoding name. such as ISO-8859-1, GBK, UTF-8. If failed, will use
     * UTF-8 as the default charset
     *
     * @param encoding the charset name
     */
    public static Charset tryParseCharset(String encoding) {
        if (StringUtils.isBlank(encoding)) {
            return StandardCharsets.UTF_8;
        }
        try {
            return Charset.forName(encoding);
        } catch (Exception e) {
            throw new UnsupportedOperationException(encoding);
        }
    }
}
