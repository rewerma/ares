package com.github.ares.connctor.jdbc.catalog.oracle;

import com.github.ares.connctor.jdbc.utils.JdbcUrlUtil;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OracleURLParser {
    private static final Pattern ORACLE_URL_PATTERN =
            Pattern.compile(
                    "^(?<url>jdbc:oracle:thin:@(//)?(?<host>[^:]+):(?<port>\\d+)[:/])(?<database>.+?)((?<suffix>\\?.*)?)$");

    public static JdbcUrlUtil.UrlInfo parse(String url) {
        Matcher matcher = ORACLE_URL_PATTERN.matcher(url);
        if (matcher.find()) {
            String urlWithoutDatabase = matcher.group("url");
            String host = matcher.group("host");
            Integer port = Integer.valueOf(matcher.group("port"));
            String database = matcher.group("database");
            String suffix = Optional.ofNullable(matcher.group("suffix")).orElse("");
            return new JdbcUrlUtil.UrlInfo(url, urlWithoutDatabase, host, port, database, suffix);
        }
        return new JdbcUrlUtil.UrlInfo(url, url, null, null, "temp", null);
    }
}
