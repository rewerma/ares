package com.github.ares.parser.utils;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.org.antlr.v4.runtime.ParserRuleContext;
import com.github.ares.org.antlr.v4.runtime.tree.TerminalNode;
import com.github.ares.parser.model.BaseOption;
import com.github.ares.parser.model.BaseSqlOption;
import com.github.ares.parser.sqlparser.model.SQLHint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PLParserUtil {

    public static final List<String> NEED_NON_SPACE = Arrays.asList(".");

    public static final List<String> REPLACE_SPACE_STRINGS = Arrays.asList(">=", "<=", "||", "/*", "*/", "*+");

    public static String getFullText(Object o) {
        return getFullSQLWithParams(o, null, null);
    }

    private static volatile String preTNode = null;

    public static String getFullSQLWithParams(Object o, Map<String, PlType> params, List<String> structs) {
        return getFullSQLWithParams(o, params, structs, true);
    }

    public static synchronized String getFullSQLWithParams(Object o, Map<String, PlType> params, List<String> structs, boolean startWithColon) {
        preTNode = null;
        return getFullSQLWithParams(o, params, structs, startWithColon, null);
    }


    public static String getFullSQLWithParams(Object o, Map<String, PlType> params, List<String> structs,
                                              boolean startWithColon, StringBuilder resStr) {
        if (o == null) {
            return "";
        }

        if (o instanceof TerminalNode) {
            String terminalNode = ((TerminalNode) o).getText();
            if (terminalNode.startsWith("'") && terminalNode.endsWith("'") && !terminalNode.equals("''")) {
                terminalNode = terminalNode.replace("''", "\\'");
            }
            if (params != null) {
                for (String param : params.keySet()) {
                    if ((terminalNode).equals((startWithColon ? ":" : "") + param)) {
                        terminalNode = "\"${" + param + "}\"";
                        break;
                    }
                }
                // if (terminalNode.startsWith(":")) {
                //     throw new ParseException("param is not defined: " + terminalNode);
                // }
            }

            if (resStr != null && resStr.length() > 2) {
                int resStrLen = resStr.length();
                for (String replaceSpaceStr : REPLACE_SPACE_STRINGS) {
                    if (terminalNode.equals(String.valueOf(replaceSpaceStr.charAt(1))) && resStr.charAt(resStrLen - 1) == ' '
                            && resStr.charAt(resStrLen - 2) == replaceSpaceStr.charAt(0)) {
                        resStr.delete(resStrLen - 2, resStrLen).append(replaceSpaceStr.charAt(0));
                    }
                }
            }
            return terminalNode;
        }

        if (o instanceof ParserRuleContext) {
            ParserRuleContext ctx = (ParserRuleContext) o;

            if (ctx.getChildCount() > 0) {
                StringBuilder sb = new StringBuilder();

                for (int i = 0; i < ctx.getChildCount(); i++) {
                    String text = getFullSQLWithParams(ctx.getChild(i), params, structs, startWithColon, sb);
                    if (NEED_NON_SPACE.contains(text)) {
                        if (sb.length() > 0 && sb.substring(sb.length() - 1).equals(" ")) {
                            sb.delete(sb.length() - 1, sb.length());
                        }
                    }
                    if (preTNode != null) {
                        String structColumn = "\"${" + preTNode + "." + text + "}\"";
                        sb.append(structColumn);
                        preTNode = null;
                    } else {
                        sb.append(text);
                    }

                    if (i != ctx.getChildCount() - 1 && !NEED_NON_SPACE.contains(text)) {
                        sb.append(" ");
                    }
                    if (text.equals(".")) {
                        String str = sb.toString().trim();
                        int idx = str.lastIndexOf(" ");
                        if (idx < 0) {
                            idx = 0;
                        } else {
                            if (startWithColon) {
                                idx += 2;
                            } else {
                                idx++;
                            }
                        }
                        String structName = str.substring(idx, str.length() - 1);
                        if (structs != null && structs.contains(structName)) {
                            preTNode = structName;
                            if (startWithColon) {
                                sb.delete(sb.length() - structName.length() - 2, sb.length());
                            } else {
                                sb.delete(sb.length() - structName.length() - 1, sb.length());
                            }
                        }
                    }
                }

                return sb.toString();
            }
            return ctx.getText();
        }
        return o.toString();
    }

    public static String getFullExprWithParams(Object o, Map<String, PlType> params, List<String> structs) {
        return getFullSQLWithParams(o, params, structs, false);
    }

    public static String getOriginalType(PlType type) {
        switch (type.getType()) {
            case LONG:
                return "BIGINT";
            case BOOLEAN:
                return "BOOLEAN";
            case INT:
                return "INT";
            case BYTE:
                return "BYTE";
            case SMALLINT:
                return "SMALLINT";
            case DOUBLE:
                return "DOUBLE";
            case FLOAT:
                return "FLOAT";
            case NUMERIC:
                return "DECIMAL";
            case VARCHAR:
                return "VARCHAR";
            case DATE:
                return "DATE";
            case TIMESTAMP:
                return "TIMESTAMP";
            case BYTES:
                return "BINARY";
            default:
                return "";
        }
    }


    public static PlType getTargetType(String type, Integer precision, Integer scale) {
        PlType targetType;

        switch (type.toUpperCase()) {
            case "INT":
                targetType = PlType.of(InternalFieldType.INT);
                break;
            case "SMALLINT":
                targetType = PlType.of(InternalFieldType.SMALLINT);
                break;
            case "BYTE":
                targetType = PlType.of(InternalFieldType.BYTE);
                break;
            case "BIGINT":
                targetType = PlType.of(InternalFieldType.LONG);
                break;
            case "BOOLEAN":
                targetType = PlType.of(InternalFieldType.BOOLEAN);
                break;
            case "DOUBLE":
                targetType = PlType.of(InternalFieldType.DOUBLE);
                break;
            case "FLOAT":
                targetType = PlType.of(InternalFieldType.FLOAT);
                break;
            case "NUMBER":
            case "DECIMAL":
                targetType = PlType.of(InternalFieldType.NUMERIC);
                if (precision != null) {
                    targetType.setPrecision(precision);
                } else {
                    targetType.setPrecision(10);
                }
                if (scale != null) {
                    targetType.setScale(scale);
                } else {
                    targetType.setScale(0);
                }
                break;
            case "STRING":
            case "VARCHAR":
            case "VARCHAR2":
                targetType = PlType.of(InternalFieldType.VARCHAR);
                break;
            case "DATE":
                targetType = PlType.of(InternalFieldType.DATE);
                break;
            case "TIMESTAMP":
                targetType = PlType.of(InternalFieldType.TIMESTAMP);
                break;
            case "BINARY":
            case "BLOB":
                targetType = PlType.of(InternalFieldType.BYTES);
                break;
            default:
                throw new ParseException("Unsupported data type: " + type);
        }

        return targetType;
    }


    public static String cleanSQL(String sql) {
        if (sql == null) {
            return null;
        }
        sql = sql.trim();
        if (sql.endsWith(";")) {
            return sql.substring(0, sql.length() - 1);
        }
        return sql;
    }

    public static String clearParam(String param) {
        if (param == null) {
            return "";
        }
        if (param.trim().startsWith("\"${") && param.trim().endsWith("}\"")) {
            return param.trim().substring(0, param.length() - 2).substring(3);
        } else {
            return param.trim();
        }
    }

    public static void setRepartition(SQLHint hint, BaseSqlOption baseSqlOption) {
        int size = hint.getArguments().size();
        try {
            if (size > 1) {
                int num = Integer.parseInt(hint.getArguments().get(0));
                baseSqlOption.setRepartitionNums(num);
                baseSqlOption.setRepartitionColumns(new ArrayList<>());
                for (int i = 1; i < size; i++) {
                    baseSqlOption.getRepartitionColumns().add(hint.getArguments().get(i));
                }
            } else if (size == 1) {
                int num = Integer.parseInt(hint.getArguments().get(0));
                baseSqlOption.setRepartitionNums(num);
            }
        } catch (Exception e) {
            // ignore
        }
    }

    public static void setShowLine(SQLHint hint, BaseOption baseOption) {
        int line = 10;
        if (!hint.getArguments().isEmpty()) {
            String arg = hint.getArguments().get(0);
            try {
                line = Integer.parseInt(arg);
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        baseOption.setWithShow(line);
    }
}
