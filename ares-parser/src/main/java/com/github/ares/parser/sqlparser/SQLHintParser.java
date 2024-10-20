package com.github.ares.parser.sqlparser;

import com.github.ares.api.common.EngineType;
import com.github.ares.api.common.ExecutionEngineType;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.sqlparser.model.SQLHint;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.ArrayList;
import java.util.List;

public class SQLHintParser {

    public static List<SQLHint> parseHints(String hints) throws ParseException {
        List<SQLHint> sqlHints = new ArrayList<>();
        String[] hintArray = hints.split(";");

        for (String hint : hintArray) {
            SQLHint sqlHint = new SQLHint();
            hint = hint.trim();
            int i = hint.indexOf("(");
            if (i < 0) {
                sqlHint.setHintName(hint);
            } else {
                if (!hint.endsWith(")")) {
                    throw new ParseException("Invalid hint format: " + hint);
                }
                sqlHint.setHintName(hint.substring(0, i).trim());
                String args = hint.substring(i + 1, hint.length() - 1).trim();
                if (!args.isEmpty()) {
                    String[] argArray = args.split(",");
                    for (String arg : argArray) {
                        sqlHint.getArguments().add(arg.trim());
                    }
                }
            }
            sqlHints.add(sqlHint);
        }
        return sqlHints;
    }

    public static List<SQLHint> parseHints(PlainSelect plainSelect) {
        if (plainSelect.getOracleHint() == null) {
            return null;
        }
        List<SQLHint> hints = parseHints(plainSelect.getOracleHint().getValue());
        OracleHint oracleHint = plainSelect.getOracleHint();
        plainSelect.setOracleHint(null);
        for (SQLHint hint : hints) {
            if (ExecutionEngineType.engineType == EngineType.SPARK) {
                if ("mapjoin".equalsIgnoreCase(hint.getHintName()) || "broadcast".equalsIgnoreCase(hint.getHintName())) {
                    String args = String.join(", ", hint.getArguments());
                    String mapJoinValue = hint.getHintName() + "(" + args + ")";
                    oracleHint.setValue(mapJoinValue);
                    plainSelect.setOracleHint(oracleHint);
                    break;
                }
            }
        }
        return hints;
    }
}
