package com.github.ares.spark.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.spark.function.utils.BinaryTypeExpression;
import com.github.ares.spark.function.utils.LongTypeExpression;
import com.github.ares.spark.function.utils.StringTypeExpression;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.unsafe.types.UTF8String;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class ParseURL implements SparkFuncInterface {
    private static String REGEXPREFIX = "(&|^)";
    private static String REGEXSUBFIX = "=([^&]*)";

    @Override
    public String functionName() {
        return "PARSE_URL";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), new int[]{2, 3}, args.size());

        String url = toStr(args.get(0));
        if (url == null) {
            return null;
        }
        String partToExtract = toStr(args.get(1));
        if (partToExtract == null) {
            return null;
        }

        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            return null;
        }
        if (args.size() == 2) {
            return parseUrlWithoutKey(uri, partToExtract);
        } else {
            Object key = args.get(2);
            if (key == null) {
                return null;
            }
            if (!"QUERY".equals(partToExtract)) {
                return null;
            }
            String query = parseUrlWithoutKey(uri, partToExtract);
            if (query == null) {
                return null;
            }
            Pattern pattern = Pattern.compile(REGEXPREFIX + key + REGEXSUBFIX);

            Matcher m = pattern.matcher(query);
            if (m.find()) {
                return UTF8String.fromString(m.group(2));
            } else {
                return null;
            }
        }
    }

    private static String parseUrlWithoutKey(URI url, String partToExtract) {
        switch (partToExtract) {
            case "HOST":
                return url.getHost();
            case "PATH":
                return url.getRawPath();
            case "QUERY":
                return url.getRawQuery();
            case "REF":
                return url.getRawFragment();
            case "PROTOCOL":
                return url.getScheme();
            case "FILE":
                if (url.getRawQuery() != null) {
                    return url.getRawPath() + "?" + url.getRawQuery();
                } else {
                    return url.getRawPath();
                }
            case "AUTHORITY":
                return url.getRawAuthority();
            case "USERINFO":
                return url.getRawUserInfo();
            default:
                return null;
        }
    }
}
